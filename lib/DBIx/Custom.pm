use 5.008007;
package DBIx::Custom;
use Object::Simple -base;

our $VERSION = '0.44';

use Carp 'confess';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Where;
use DBIx::Custom::Model;
use DBIx::Custom::Order;
use DBIx::Custom::Util qw/_array_to_hash _subname _deprecate/;
use DBIx::Custom::Mapper;
use DBIx::Custom::NotExists;
use DBIx::Custom::Query;
use DBIx::Connector;

use Encode qw/encode encode_utf8 decode_utf8/;
use Scalar::Util qw/weaken/;

has [qw/dsn password quote user exclude_table user_table_info
     user_column_info safety_character/];
has connector => 1;
has option => sub { {} };
has default_option => sub {
  {
    RaiseError => 1,
    PrintError => 0,
    AutoCommit => 1
  }
};
has filters => sub {
  {
    encode_utf8 => sub { encode_utf8($_[0]) },
    decode_utf8 => sub { decode_utf8($_[0]) }
  }
};
has last_sql => '';
has models => sub { {} };
has now => sub {
  sub {
    my ($sec, $min, $hour, $mday, $mon, $year) = localtime;
    $mon++;
    $year += 1900;
    my $now = sprintf("%04d-%02d-%02d %02d:%02d:%02d",
      $year, $mon, $mday, $hour, $min, $sec);
    return $now;
  }
};
has result_class  => 'DBIx::Custom::Result';
has separator => '.';

has mytable_symbol => '__MY__';

sub create_result {
  my ($self, $sth) = @_;
  
  return $self->result_class->new(sth => $sth, dbi => $self);
}

sub column {
  my $self = shift;
  my $option = pop if ref $_[-1] eq 'HASH';
  my $real_table = shift;
  my $columns = shift;
  my $table = $option->{alias} || $real_table;
  
  # Columns
  if (!defined $columns || $columns eq '*') {
    $columns = $self->model($real_table)->columns;
  }
  
  # Separator
  my $separator = $self->separator;
  
  # . is replaced
  my $t = $table;
  $t =~ s/\./$separator/g;
  
  # Column clause
  my @column;
  $columns ||= [];
  push @column, $self->_tq($table) . "." . $self->q($_) .
    " as " . $self->q("${t}${separator}$_")
    for @$columns;
  
  return join (', ', @column);
}

sub connect {
  my $self;
  
  if (ref $_[0]) {
    $self = shift;
  }
  else {
    $self = shift->new(@_);
  }
  
  my $connector = $self->connector;
  
  if (!ref $connector && $connector) {
    my $dsn = $self->dsn;
    my $user = $self->user;
    my $password = $self->password;
    my $option = $self->option;
    my $connector = DBIx::Connector->new($dsn, $user, $password,
      {%{$self->default_option} , %$option});
    $self->connector($connector);
  }
  
  # Connect
  $self->dbh;
  
  return $self;
}

sub dbh {
  my $self = shift;
  
  # Set
  if (@_) {
    $self->{dbh} = $_[0];
    
    return $self;
  }
  
  # Get
  else {
    # From Connction manager
    if (my $connector = $self->connector) {
      confess "connector must have dbh() method " . _subname
        unless ref $connector && $connector->can('dbh');
        
      $self->{dbh} = $connector->dbh;
    }
    
    # Connect
    $self->{dbh} ||= $self->_connect;
    
    # Quote
    if (!defined $self->quote) {
      my $driver = $self->_driver;
      my $quote =  $driver eq 'odbc' ? '[]'
        : $driver eq 'ado' ? '[]'
        : $driver eq 'mysql' ? '`'
        : '"';
      $self->quote($quote);
    }
    
    return $self->{dbh};
  }
}

sub delete {
  my ($self, %opt) = @_;
  
  # Don't allow delete all rows
  confess qq{delete method where or id option must be specified } . _subname
    if !$opt{where} && !defined $opt{id} && !$opt{allow_delete_all};
  
  # Where
  my $where;
  if (defined $opt{id}) {
    $where = $self->_id_to_param($opt{id}, $opt{primary_key}, $opt{table}) ;
  }
  else {
    $where = $opt{where};
  }
  my $w = $self->_where_clause_and_param($where);
  
  # Delete statement
  my $sql = "delete ";
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  $sql .= "from " . $self->_tq($opt{table}) . " $w->{clause} ";
  
  # Execute query
  $self->execute($sql, $w->{param}, %opt);
}

sub delete_all { shift->delete(@_, allow_delete_all => 1) }

sub create_model {
  my $self = shift;
  
  my $opt;
  if (@_ % 2 != 0 && !ref $_[0]) {
    $opt = {table => shift, @_};
  }
  else {
    $opt = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  }
  
  # Options
  $opt->{dbi} = $self;
  my $model_class = delete $opt->{model_class} || 'DBIx::Custom::Model';
  my $model_name  = delete $opt->{name};
  my $model_table = delete $opt->{table};
  $model_name ||= $model_table;
  my $column_name_lc = delete $opt->{column_name_lc};
  
  # Create model
  my $model = $model_class->new($opt);
  weaken $model->{dbi};
  $model->table($model_table) unless $model->table;
  $model->name($model_name);

  if (!$model->columns || !@{$model->columns}) {
    $model->columns($self->get_columns_from_db($model->table, {column_name_lc => $column_name_lc}));
  }
  
  # Set model
  $self->model($model_name, $model);
  
  return $self->model($model->name);
}

sub execute {
  my $self = shift;
  my $sql = shift;
  
  # Options
  my $param;
  $param = shift if @_ % 2;
  $param ||= {};
  my %opt = @_;
  
  # Append
  $sql .= $opt{append} if defined $opt{append};

  # Parse named place holder
  my $safe_char = $self->{safety_character};
  my $place_holder_re = $safe_char eq 'a-zA-Z0-9_'
    ? qr/(.*?[^\\]):([$safe_char\.]+)(?:\{(.*?)\})?(.*)/so
    : qr/(.*?[^\\]):([$safe_char\.]+)(?:\{(.*?)\})?(.*)/s;
  my $source_sql = $sql;
  $source_sql =~ s/([0-9]):/$1\\:/g;
  my $parsed_sql = '';
  my $columns;
  while ($source_sql =~ /$place_holder_re/) {
    push @$columns, $2;
    ($parsed_sql, $source_sql) = defined $3 ?
      ($parsed_sql . "$1$2 $3 ?", " $4") : ($parsed_sql . "$1?", " $4");
  }
  $parsed_sql .= $source_sql;
  $parsed_sql =~ s/\\:/:/g if index($parsed_sql, "\\:") != -1;
  
  # Edit SQL after building
  my $after_build_sql = $opt{after_build_sql};
  $parsed_sql = $after_build_sql->($parsed_sql) if $after_build_sql;
  
  # Type rule
  my $type_filters;
  if ($self->{_type_rule_is_called}) {
    $type_filters = {};
    unless ($opt{type_rule_off}) {
      my $tables = $opt{table} || [];
      $tables = [$tables] unless ref $tables eq 'ARRAY';

      # Tables
      my $main_table = @{$tables}[-1];
      
      my $type_rule_off_parts = {
        1 => $opt{type_rule1_off},
        2 => $opt{type_rule2_off}
      };
      for my $i (1, 2) {
        unless ($type_rule_off_parts->{$i}) {
          $type_filters->{$i} = {};
          my $table_alias = $opt{table_alias} || {};
          for my $alias (keys %$table_alias) {
            my $table = $table_alias->{$alias};
            
            for my $column (keys %{$self->{"_into$i"}{key}{$table} || {}}) {
              $type_filters->{$i}->{"$alias.$column"} = $self->{"_into$i"}{key}{$table}{$column};
            }
          }
          $type_filters->{$i} = {%{$type_filters->{$i}}, %{$self->{"_into$i"}{key}{$main_table} || {}}}
            if $main_table;
        }
      }
    }
  }
  
  # Replace filter name to code
  my $filter = $opt{filter};
  if (defined $filter) {
    if (ref $opt{filter} eq 'ARRAY') {
      $filter = _array_to_hash($filter);
    }
    
    for my $column (keys %$filter) {
      my $name = $filter->{$column};
      if (!defined $name) {
        $filter->{$column} = undef;
      }
      elsif (ref $name ne 'CODE') {
        confess qq{Filter "$name" is not registered" } . _subname
          unless exists $self->filters->{$name};
        $filter->{$column} = $self->filters->{$name};
      }
    }
  }
  
  # Bind type
  my $bind_type = $opt{bind_type};
  $bind_type = _array_to_hash($bind_type) if ref $bind_type eq 'ARRAY';
  
  # Create query
  my $query = DBIx::Custom::Query->new;
  $query->param($param);
  $query->sql($parsed_sql);
  $query->columns($columns);
  $query->bind_type($bind_type);
  
  $query->{_filter} = $filter;
  $query->{_type_filters} = $type_filters;
  $query->{_into1} = $self->{_into1};
  $query->{_into2} = $self->{_into2};
  
  # Has filter
  $query->{_f} = defined $filter || defined $type_filters;
  
  # Build bind values
  $query->build;
  
  # Statement handle
  my $sth;
  my $reuse_sth;
  $reuse_sth = $opt{reuse}->{$parsed_sql} if $opt{reuse};
  if ($reuse_sth) {
    $sth = $reuse_sth;
  }
  else {
    # Prepare statement handle
    eval { $sth = $self->dbh->prepare($parsed_sql) };
    if ($@) {
      $self->_confess($@, qq{. Following SQL is executed.\n}
                      . qq{$parsed_sql\n} . _subname);
    }
    $opt{reuse}->{$parsed_sql} = $sth if $opt{reuse};
  }
  
  # Execute
  my $affected;
  eval {
    my $bind_values = $query->bind_values;
    if ($bind_type) {
      my $bind_value_types = $query->bind_value_types;
      $sth->bind_param($_ + 1, $bind_values->[$_],
          $bind_value_types->[$_] ? $bind_value_types->[$_] : ())
        for (0 .. @$bind_values - 1);
      $affected = $sth->execute;
    }
    else { $affected = $sth->execute(@$bind_values) }
    
    # Save sql
    $self->{last_sql} = $parsed_sql;
    
    # DEBUG message
    if ($ENV{DBIX_CUSTOM_DEBUG}) {
      warn "SQL:\n" . $parsed_sql . "\n";
      my @output;
      for my $value (@$bind_values) {
        $value = 'undef' unless defined $value;
        $value = encode($ENV{DBIX_CUSTOM_DEBUG_ENCODING} || 'UTF-8', $value)
          if utf8::is_utf8($value);
        push @output, $value;
      }
      warn "Bind values: " . join(', ', @output) . "\n\n";
    }
  };
  
  $self->_confess($@, qq{. Following SQL is executed.\n}
    . qq{$parsed_sql\n} . _subname) if $@;
  
  # Reulst of select statement
  if ($sth->{NUM_OF_FIELDS}) {
    # Result
    my $result = $self->result_class->new(
      sth => $sth,
      dbi => $self,
    );
    
    if ($self->{_type_rule_is_called}) {
      $result->type_rule({
        from1 => $self->type_rule->{from1},
        from2 => $self->type_rule->{from2}
      });
      $result->{_has_filter} = 1;
    }
    
    return $result;
  }
  # Affected of insert, update, or delete
  else {
    return $affected
  }
}

sub include_model {
  my ($self, $name_space, $model_infos) = @_;
  
  # Name space
  $name_space ||= '';
  
  # Get Model information
  unless ($model_infos) {

    # Load name space module
    confess qq{"$name_space" is invalid class name } . _subname
      if $name_space =~ /[^\w:]/;
    eval "use $name_space";
    confess qq{Name space module "$name_space.pm" is needed. $@ } . _subname
      if $@;
    
    # Search model modules
    my $name_space_dir = $name_space;
    $name_space_dir =~ s/::/\//g;
    my $path = $INC{"$name_space_dir.pm"};
    $path =~ s/\.pm$//;
    opendir my $dh, $path
      or confess qq{Can't open directory "$path": $! } . _subname
    my @modules;
    while (my $file = readdir $dh) {
      my $file_abs = "$path/$file";
      if (-d $file_abs) {
        next if $file eq '.' || $file eq '..';
        opendir my $fq_dh, $file_abs
          or confess qq{Can't open directory "$file_abs": $! } . _subname;
        while (my $fq_file = readdir $fq_dh) {
          my $fq_file_abs = "$file_abs/$fq_file";
          push @modules, "${file}::$fq_file" if -f $fq_file_abs;
        }
        close $fq_dh;
      }
      elsif(-f $file_abs) { push @modules, $file }
    }
    close $dh;
    
    $model_infos = [];
    for my $module (@modules) {
      if ($module =~ s/\.pm$//) { push @$model_infos, $module }
    }
  }
  
  # Include models
  for my $model_info (@$model_infos) {
    
    # Load model
    my $model_class;
    my $model_name;
    my $model_table;
    if (ref $model_info eq 'HASH') {
      $model_class = $model_info->{class};
      $model_name  = $model_info->{name};
      $model_table = $model_info->{table};
      
      $model_name  ||= $model_class;
      $model_table ||= $model_name;
    }
    else {
      $model_class = $model_name = $model_table = $model_info;
    }

    $model_class =~ s/\./::/g;
    $model_name =~ s/::/./;
    $model_table =~ s/::/./;

    my $mclass = "${name_space}::$model_class";
    confess qq{"$mclass" is invalid class name } . _subname
      if $mclass =~ /[^\w:]/;
    unless ($mclass->can('new')) {
      eval "require $mclass";
      confess "$@ " . _subname if $@;
    }
    
    # Create model
    my $opt = {};
    $opt->{model_class} = $mclass if $mclass;
    $opt->{name}        = $model_name if $model_name;
    $opt->{table}       = $model_table if $model_table;
    
    $self->create_model($opt);
    1;
  }
  
  return $self;
}

sub like_value { sub { "%$_[0]%" } }

sub mapper {
  my $self = shift;
  return DBIx::Custom::Mapper->new(@_);
}

sub merge_param {
  my ($self, $param1, $param2) = @_;
  
  # Merge parameters
  my $merged_param = {%$param1};
  for my $column (keys %$param2) {
    if (exists $merged_param->{$column}) {
      $merged_param->{$column} = [$merged_param->{$column}]
        unless ref $merged_param->{$column} eq 'ARRAY';
      push @{$merged_param->{$column}},
        ref $param2->{$column} eq 'ARRAY' ? @{$param2->{$column}} : $param2->{$column};
    }
    else { $merged_param->{$column} = $param2->{$column} }
  }
  
  return $merged_param;
}

sub model {
  my ($self, $name, $model) = @_;
  
  # Set model
  if ($model) {
    $self->models->{$name} = $model;
    return $self;
  }
  
  # Check model existence
  confess qq{Model "$name" is not yet created } . _subname
    unless $self->models->{$name};
  
  # Get model
  return $self->models->{$name};
}

sub mycolumn {
  my ($self, $table, $columns) = @_;
  
  if (!$columns || $columns eq '*') {
    $columns = $self->model($table)->columns;
  }

  # Create column clause
  my @column;
  push @column, $self->_tq($table) . "." . $self->q($_) . " as " . $self->q($_)
    for @$columns;
  
  return join (', ', @column);
}

sub new {
  my $self = shift;
  
  # Same as DBI connect argument
  if (@_ > 0 && !ref $_[0] && $_[0] =~ /:/) {
    my $dsn = shift;
    my $user = shift;
    my $password = shift;
    my $dbi_option = shift || {};
    my $attrs = shift || {};
    $attrs->{dsn} = $dsn;
    $attrs->{user} = $user;
    $attrs->{password} = $password;
    $attrs->{option} = $dbi_option;
    $self = $self->SUPER::new($attrs);
  }
  else {
    $self = $self->SUPER::new(@_);
  }
  
  # Check attributes
  my @attrs = keys %$self;
  for my $attr (@attrs) {
    confess qq{Invalid attribute: "$attr" } . _subname
      unless $self->can($attr);
  }
  
  $self->{safety_character} = 'a-zA-Z0-9_'
    unless exists $self->{safety_character};
  
  return $self;
}

sub not_exists { DBIx::Custom::NotExists->singleton }

sub order {
  my $self = shift;
  return DBIx::Custom::Order->new(dbi => $self, @_);
}

sub q { shift->_tq($_[0], $_[1], whole => 1) }

sub _tq {
  my ($self, $value, $quotemeta, %opt) = @_;
  
  my $quote = $self->{quote} || $self->quote || '';
  
  my $q = substr($quote, 0, 1) || '';
  my $p;
  if (defined $quote && length $quote > 1) {
    $p = substr($quote, 1, 1);
  }
  else { $p = $q }
  
  if ($quotemeta) {
    $q = quotemeta($q);
    $p = quotemeta($p);
  }
  
  if ($opt{whole}) { return "$q$value$p" }
  else {
    my @values = split /\./, $value;
    push @values, '' unless @values;
    for my $v (@values) { $v = "$q$v$p" }
    return join '.', @values;
  }
}

sub register_filter {
  my $self = shift;
  
  # Register filter
  my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  $self->filters({%{$self->filters}, %$filters});
  
  return $self;
}

sub select {
  my $self = shift;
  my $column = shift if @_ % 2;
  my %opt = @_;
  $opt{column} = $column if defined $column;

  # Table
  my $table = $opt{table};
  
  # Found tables;
  my $found_tables = [];
  push @$found_tables, $table if defined $table;
  
  my $param = delete $opt{param} || {};
  
  # Select statement
  my $sql = 'select ';
  
  # Prefix
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  
  # Column
  if (defined $opt{column}) {
    my $columns
      = ref $opt{column} eq 'ARRAY' ? $opt{column} : [$opt{column}];
    for my $column (@$columns) {
      if (ref $column eq 'HASH') {
        my $mytable_symbol = $opt{mytable_symbol} || $self->mytable_symbol;
        my $table = (keys %$column)[0];
        my $columns = $column->{$table};
        
        if ($table eq $mytable_symbol) {
          $column = $self->mycolumn($found_tables->[0] => $columns);
        }
        else {
          $column = $self->column($table => $columns);
        }
      }
      unshift @$found_tables, @{$self->_search_tables($column)} if $table;
      $sql .= "$column, ";
    }
    $sql =~ s/, $/ /;
  }
  else { $sql .= '* ' }

  # Execute query without table
  return $self->execute($sql, {}, %opt) unless $table;

  # Table
  $sql .= 'from ';
  $sql .= $self->_tq($found_tables->[-1] || '') . ' ';
  $sql =~ s/, $/ /;

  # Add tables in parameter
  unshift @$found_tables, @{$self->_search_tables(join(' ', keys %$param) || '')};
  
  # Where
  my $where;
  if (defined $opt{id}) {
    $where = $self->_id_to_param($opt{id}, $opt{primary_key}, @$found_tables ? $found_tables->[-1] : undef) ;
  }
  else {
    $where = $opt{where};
  }
  my $w = $self->_where_clause_and_param($where, $opt{id});
  $param = $self->merge_param($param, $w->{param});
  
  # Search table names in where clause
  unshift @$found_tables, @{$self->_search_tables($w->{clause})};
  
  # Search table names in append option
  if (defined(my $append = $opt{append})) {
    unshift @$found_tables, @{$self->_search_tables($append)};
  }
  
  # Join statement
  my $join = [];
  if (defined $opt{join}) {
    my $opt_join = $opt{join};
    if (ref $opt_join eq 'ARRAY') {
      push @$join, @$opt_join;
    }
    else { push @$join, $opt_join }
  }
  if (defined $w->{join}) {
    my $where_join = $w->{join};
    if (ref $where_join eq 'ARRAY') {
      push @$join, @$where_join;
    }
    else { push @$join, $where_join }
  }
  $self->_push_join(\$sql, $join, $found_tables) if @$join;
  
  # Add where clause
  $sql .= "$w->{clause} ";
  
  # Execute query
  return $self->execute($sql, $param, %opt);
}

sub setup_model {
  my ($self, %opt) = @_;
  
  _deprecate('0.39', "DBIx::Custom::setup method is DEPRECATED! columns is automatically set when create_model or include_model is called");
  
  return $self;
}

sub insert {
  my $self = shift;
  
  # Options
  my $params = @_ % 2 ? shift : undef;
  my %opt = @_;
  $params ||= {};

  # Insert statement
  my $sql = "insert ";
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  $sql .= "into " . $self->_tq($opt{table}) . " ";

  my $multi;
  if (ref $params eq 'ARRAY') { $multi = 1 }
  else { $params = [$params] }
  
  # Created time and updated time
  if (defined $opt{ctime} || defined $opt{mtime}) {
    
    for my $param (@$params) {
      $param = {%$param};
    }
    my $now = $self->now;
    $now = $now->() if ref $now eq 'CODE';
    if (defined $opt{ctime}) {
      $_->{$opt{ctime}} = $now for @$params;
    }
    if (defined $opt{mtime}) {
      $_->{$opt{mtime}} = $now for @$params;
    }
  }
  
  # Merge id to parameter
  if (defined $opt{id} && !$multi) {
    
    _deprecate('0.39', "DBIx::Custom::insert method's id option is DEPRECATED!");
    
    for my $param (@$params) {
      $param = {%$param};
    }
    
    confess "insert id option must be specified with primary_key option"
      unless $opt{primary_key};
    $opt{primary_key} = [$opt{primary_key}] unless ref $opt{primary_key} eq 'ARRAY';
    $opt{id} = [$opt{id}] unless ref $opt{id} eq 'ARRAY';
    for (my $i = 0; $i < @{$opt{primary_key}}; $i++) {
      my $key = $opt{primary_key}->[$i];
      next if exists $params->[0]->{$key};
      $params->[0]->{$key} = $opt{id}->[$i];
    }
  }
  
  if ($opt{bulk_insert}) {
    $sql .= $self->_multi_values_clause($params, {wrap => $opt{wrap}}) . " ";
    my $new_param = {};
    $new_param->{$_} = [] for keys %{$params->[0]};
    for my $param (@$params) {
      push @{$new_param->{$_}}, $param->{$_} for keys %$param;
    }
    $params = [$new_param];
  }
  else {
    $sql .= $self->values_clause($params->[0], {wrap => $opt{wrap}}) . " ";
  }
  
  # Execute query
  if (@$params > 1) {
    for my $param (@$params) {
      $self->execute($sql, $param, %opt);
    }
  }
  else {
    $self->execute($sql, $params->[0], %opt);
  }
}

sub update {
  my $self = shift;

  # Options
  my $param = @_ % 2 ? shift : undef;
  my %opt = @_;
  $param ||= {};
  
  # Don't allow update all rows
  confess qq{update method where option must be specified } . _subname
    if !$opt{where} && !defined $opt{id} && !$opt{allow_update_all};
  
  # Created time and updated time
  if (defined $opt{mtime}) {
    $param = {%$param};
    my $now = $self->now;
    $now = $now->() if ref $now eq 'CODE';
    $param->{$opt{mtime}} = $self->now->();
  }

  # Assign clause
  my $assign_clause = $self->assign_clause($param, {wrap => $opt{wrap}});
  
  # Where
  my $where;
  if (defined $opt{id}) {
    $where = $self->_id_to_param($opt{id}, $opt{primary_key}, $opt{table}) ;
  }
  else {
    $where = $opt{where};
  }
  
  my $w = $self->_where_clause_and_param($where);
  
  # Merge update parameter with where parameter
  $param = $self->merge_param($param, $w->{param});
  
  # Update statement
  my $sql = "update ";
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  $sql .= $self->_tq($opt{table}) . " set $assign_clause $w->{clause} ";
  
  # Execute query
  $self->execute($sql, $param, %opt);
}

sub update_all { shift->update(@_, allow_update_all => 1) };

sub values_clause {
  my ($self, $param, $opts) = @_;
  
  my $wrap = $opts->{wrap} || {};
  
  # Create insert parameter tag
  my ($q, $p) = $self->_qp;
  
  my $safety_character = $self->safety_character;
  
  my @columns;
  my @place_holders;
  for my $column (sort keys %$param) {
    confess qq{"$column" is not safety column name in values clause} . _subname
      unless $column =~ /^[$safety_character\.]+$/;

    push @columns, "$q$column$p";
    push @place_holders, ref $param->{$column} eq 'SCALAR' ? ${$param->{$column}} :
      $wrap->{$column} ? $wrap->{$column}->(":$column") :
      ":$column";
  }
  
  my $values_clause = '(' . join(', ', @columns) . ') values (' . join(', ', @place_holders) . ')';
  
  return $values_clause;
}

sub assign_clause {
  my ($self, $param, $opts) = @_;
  
  my $wrap = $opts->{wrap} || {};
  my ($q, $p) = $self->_qp;

  my $safety_character = $self->safety_character;

  my @set_values;
  for my $column (sort keys %$param) {
    confess qq{"$column" is not safety column name in assign clause} . _subname
      unless $column =~ /^[$safety_character\.]+$/;
      
    push @set_values, ref $param->{$column} eq 'SCALAR' ? "$q$column$p = " . ${$param->{$column}}
      : $wrap->{$column} ? "$q$column$p = " . $wrap->{$column}->(":$column")
      : "$q$column$p = :$column";
  }
  
  my $assign_clause = join(', ', @set_values);
  
  return $assign_clause;
}

sub where { DBIx::Custom::Where->new(dbi => shift, @_) }

sub type_rule {
  my $self = shift;

  $self->{_type_rule_is_called} = 1;
  
  if (@_) {
    my $type_rule = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    # Into
    for my $i (1 .. 2) {
      my $into = "into$i";
      my $exists_into = exists $type_rule->{$into};
      $type_rule->{$into} = _array_to_hash($type_rule->{$into});
      $self->{type_rule} = $type_rule;
      $self->{"_$into"} = {};
      for my $type_name (keys %{$type_rule->{$into} || {}}) {
        confess qq{type name of $into section must be lower case}
          if $type_name =~ /[A-Z]/;
      }
      
      $self->each_column(sub {
        my ($dbi, $table, $column, $column_info) = @_;
        
        my $type_name = lc $column_info->{TYPE_NAME};
        if ($type_rule->{$into} &&
            (my $filter = $type_rule->{$into}->{$type_name}))
        {
          return unless exists $type_rule->{$into}->{$type_name};
          if (defined $filter && ref $filter ne 'CODE') 
          {
            my $fname = $filter;
            confess qq{Filter "$fname" is not registered" } . _subname
              unless exists $self->filters->{$fname};
            
            $filter = $self->filters->{$fname};
          }
          
          my $schema = $column_info->{TABLE_SCHEM};
          $self->{"_$into"}{key}{$table}{$column} = $filter;
          $self->{"_$into"}{dot}{"$table.$column"} = $filter;
          
          $self->{"_$into"}{key}{"$schema.$table"}{$column} = $filter;
          $self->{"_$into"}{dot}{"$schema.$table.$column"} = $filter;
        }
      });
    }

    # From
    for my $i (1 .. 2) {
      $type_rule->{"from$i"} = _array_to_hash($type_rule->{"from$i"});
      for my $data_type (keys %{$type_rule->{"from$i"} || {}}) {
        confess qq{data type of from$i section must be lower case or number}
          if $data_type =~ /[A-Z]/;
        my $fname = $type_rule->{"from$i"}{$data_type};
        if (defined $fname && ref $fname ne 'CODE') {
          confess qq{Filter "$fname" is not registered" } . _subname
            unless exists $self->filters->{$fname};
          
          $type_rule->{"from$i"}{$data_type} = $self->filters->{$fname};
        }
      }
    }
    
    return $self;
  }
  
  return $self->{type_rule} || {};
}

sub get_table_info {
  my ($self, %opt) = @_;
  
  my $exclude = delete $opt{exclude};
  confess qq/"$_" is wrong option/ for keys %opt;
  
  my $table_info = [];
  $self->each_table(
    sub { push @$table_info, {table => $_[1], info => $_[2] } },
    exclude => $exclude
  );
  
  return [sort {$a->{table} cmp $b->{table} } @$table_info];
}

sub get_column_info {
  my ($self, %opt) = @_;
  
  my $exclude_table = delete $opt{exclude_table};
  confess qq/"$_" is wrong option/ for keys %opt;
  
  my $column_info = [];
  $self->each_column(
    sub { push @$column_info, {table => $_[1], column => $_[2], info => $_[3] } },
    exclude_table => $exclude_table
  );
  
  return [
    sort {$a->{table} cmp $b->{table} || $a->{column} cmp $b->{column} }
      @$column_info];
}

sub each_column {
  my ($self, $cb, %options) = @_;
  
  my $user_column_info = $self->user_column_info;
  
  if ($user_column_info) {
    $self->$cb($_->{table}, $_->{column}, $_->{info}) for @$user_column_info;
  }
  else {
    my $re = $self->exclude_table || $options{exclude_table};
    # Tables
    my $tables = {};
    $self->each_table(sub {
      my ($dbi, $table, $table_info) = @_;
      my $schema = $table_info->{TABLE_SCHEM};
      $tables->{$schema}{$table}++;
    });

    # Iterate all tables
    for my $schema (sort keys %$tables) {
      for my $table (sort keys %{$tables->{$schema}}) {
        
        # Iterate all columns
        my $sth_columns;
        eval {$sth_columns = $self->dbh->column_info(undef, $schema, $table, '%')};
        next if $@;
        while (my $column_info = $sth_columns->fetchrow_hashref) {
          my $column = $column_info->{COLUMN_NAME};
          $self->$cb($table, $column, $column_info);
        }
      }
    }
  }
}

sub get_columns_from_db {
  my ($self, $schema_table, $opt) = @_;

  $opt ||= {};
  
  my $column_name_lc = $opt->{column_name_lc};
  
  my $schema;
  my $table;
  if ($schema_table =~ /^(.+)\.(.*)$/) {
    $schema = $1;
    $table = $2;
  }
  else {
    $schema = undef;
    $table = $schema_table;
  }
  
  my $sth_columns;
  eval {$sth_columns = $self->dbh->column_info(undef, $schema, $table, "%") };
  if ($@) {
    return;
  }
  
  my $columns;
  while (my $column_info = $sth_columns->fetchrow_hashref) {
    $columns ||= [];
    my $column = $column_info->{COLUMN_NAME};
    if ($column_name_lc) {
      $column = lc $column;
    }
    push @$columns, $column;
  }
  
  return $columns;
}

sub each_table {
  my ($self, $cb, %option) = @_;
  
  my $user_table_infos = $self->user_table_info;
  
  # Iterate tables
  if ($user_table_infos) {
      $self->$cb($_->{table}, $_->{info}) for @$user_table_infos;
  }
  else {
    my $re = $self->exclude_table || $option{exclude};
    my $sth_tables = $self->dbh->table_info;
    while (my $table_info = $sth_tables->fetchrow_hashref) {
      # Table
      my $table = $table_info->{TABLE_NAME};
      next if defined $re && $table =~ /$re/;
      $self->$cb($table, $table_info);
    }
  }
}

sub available_datatype {
  my $self = shift;
  
  my $data_types = '';
  for my $i (-1000 .. 1000) {
     my $type_info = $self->dbh->type_info($i);
     my $data_type = $type_info->{DATA_TYPE};
     my $type_name = $type_info->{TYPE_NAME};
     $data_types .= "$data_type ($type_name)\n"
       if defined $data_type;
  }
  return "Data Type maybe equal to Type Name" unless $data_types;
  $data_types = "Data Type (Type name)\n" . $data_types;
  return $data_types;
}

sub available_typename {
  my $self = shift;
  
  # Type Names
  my $type_names = {};
  $self->each_column(sub {
    my ($self, $table, $column, $column_info) = @_;
    $type_names->{$column_info->{TYPE_NAME}} = 1
      if $column_info->{TYPE_NAME};
  });
  my @output = sort keys %$type_names;
  unshift @output, "Type Name";
  return join "\n", @output;
}

sub show_datatype {
  my ($self, $table) = @_;
  confess "Table name must be specified" unless defined $table;
  print "$table\n";
  
  my $result = $self->select(table => $table, where => "'0' <> '0'");
  my $sth = $result->sth;

  my $columns = $sth->{NAME};
  my $data_types = $sth->{TYPE};
  
  for (my $i = 0; $i < @$columns; $i++) {
    my $column = $columns->[$i];
    my $data_type = lc $data_types->[$i];
    print "$column: $data_type\n";
  }
}

sub show_typename {
  my ($self, $t) = @_;
  confess "Table name must be specified" unless defined $t;
  print "$t\n";
  
  $self->each_column(sub {
    my ($self, $table, $column, $infos) = @_;
    return unless $table eq $t;
    my $typename = lc $infos->{TYPE_NAME};
    print "$column: $typename\n";
  });
  
  return $self;
}

sub show_tables {
  my $self = shift;
  
  my %tables;
  $self->each_table(sub { $tables{$_[1]}++ });
  print join("\n", sort keys %tables) . "\n";
  return $self;
}

sub _qp {
  my ($self, %opt) = @_;

  my $quote = $self->{quote} || $self->quote || '';
  
  my $q = substr($quote, 0, 1) || '';
  my $p;
  if (defined $quote && length $quote > 1) {
    $p = substr($quote, 1, 1);
  }
  else { $p = $q }
  
  if ($opt{quotemeta}) {
    $q = quotemeta($q);
    $p = quotemeta($p);
  }
  
  return ($q, $p);
}

sub _multi_values_clause {
  my ($self, $params, $opts) = @_;
  
  my $wrap = $opts->{wrap} || {};
  
  # Create insert parameter tag
  my ($q, $p) = $self->_qp;
  
  my $safety_character = $self->safety_character;
  
  my $first_param = $params->[0];
  
  my @columns;
  my @columns_quoted;
  for my $column (keys %$first_param) {
    confess qq{"$column" is not safety column name in multi values clause} . _subname
      unless $column =~ /^[$safety_character\.]+$/;
    
    push @columns, $column;
    push @columns_quoted, "$q$column$p";
  }

  # Multi values clause
  my $multi_values_clause = '(' . join(', ', @columns_quoted) . ') values ';

  for my $param (@$params) {
    my @place_holders;
    for my $column (@columns) {
      push @place_holders, ref $param->{$column} eq 'SCALAR' ? ${$param->{$column}} :
        $wrap->{$column} ? $wrap->{$column}->(":$column") :
        ":$column";
    }
    $multi_values_clause .= '(' . join(', ', @place_holders) . '), ';
  }
  $multi_values_clause =~ s/, $//;
  
  return $multi_values_clause;
}

sub _id_to_param {
  my ($self, $id, $primary_keys, $table) = @_;
  
  # Check primary key
  confess "primary_key option " .
        "must be specified when id option is used" . _subname
    unless defined $primary_keys;
  $primary_keys = [$primary_keys] unless ref $primary_keys eq 'ARRAY';
  
  _deprecate('0.39', "DBIx::Custom::select,update,delete method's id and primary_key option is DEPRECATED!");
  
  # Create parameter
  my $param = {};
  if (defined $id) {
    $id = [$id] unless ref $id eq 'ARRAY';
    for(my $i = 0; $i < @$id; $i++) {
      my $key = $primary_keys->[$i];
      $key = "$table." . $key if $table;
      $param->{$key} = $id->[$i];
    }
  }
  
  return $param;
}

sub _connect {
  my $self = shift;
  
  # Attributes
  my $dsn = $self->dsn;
  confess qq{"dsn" must be specified } . _subname
    unless $dsn;
  my $user        = $self->user;
  my $password    = $self->password;
  my $option = $self->option;
  $option = {%{$self->default_option}, %$option};
  
  # Connect
  my $dbh;
  eval { $dbh = DBI->connect($dsn, $user, $password, $option) };
  
  # Connect error
  confess "$@ " . _subname if $@;
  
  return $dbh;
}

sub _confess {
  my ($self, $error, $append) = @_;
  
  # Append
  $append ||= "";
  
  # Verbose
  if ($Carp::Verbose) { confess $error }
  
  # Not verbose
  else {
    # Remove line and module information
    my $at_pos = rindex($error, ' at ');
    $error = substr($error, 0, $at_pos);
    $error =~ s/\s+$//;
    confess "$error$append";
  }
}

sub _driver { lc shift->{dbh}->{Driver}->{Name} }

sub _need_tables {
  my ($self, $tree, $need_tables, $tables) = @_;
  
  # Get needed tables
  for my $table (@$tables) {
    if ($tree->{$table}) {
      $need_tables->{$table} = 1;
      $self->_need_tables($tree, $need_tables, [$tree->{$table}{parent}])
    }
  }
}

sub _push_join {
  my ($self, $sql, $join, $join_tables) = @_;
  
  # Push join clause
  my $tree = {};
  for (my $i = 0; $i < @$join; $i++) {
    
    # Arrange
    my $join_clause;;
    my $option;
    if (ref $join->[$i] eq 'HASH') {
      $join_clause = $join->[$i]->{clause};
      $option = {table => $join->[$i]->{table}};
    }
    else {
      $join_clause = $join->[$i];
      $option = {};
    };

    # Find tables in join clause
    my $table1;
    my $table2;
    if (my $table = $option->{table}) {
      $table1 = $table->[0];
      $table2 = $table->[1];
    }
    else {
      my $q = $self->_quote;
      my $j_clause = (split /\s+on\s+/, $join_clause)[-1];
      $j_clause =~ s/'.+?'//g;
      my $q_re = quotemeta($q);
      $j_clause =~ s/[$q_re]//g;
      
      my @j_clauses = reverse split /\s(and|on)\s/, $j_clause;
      my $c = $self->{safety_character};
      my $join_re = qr/((?:[$c]+?\.[$c]+?)|(?:[$c]+?))\.[$c]+[^$c].*?((?:[$c]+?\.[$c]+?)|(?:[$c]+?))\.[$c]+/sm;
      for my $clause (@j_clauses) {
        if ($clause =~ $join_re) {
          $table1 = $1;
          $table2 = $2;
          last;
        }                
      }
    }
    confess qq{join clause must have two table name after "on" keyword. } .
        qq{"$join_clause" is passed }  . _subname
      unless defined $table1 && defined $table2;
    confess qq{right side table of "$join_clause" must be unique } . _subname
      if exists $tree->{$table2};
    confess qq{Same table "$table1" is specified} . _subname
      if $table1 eq $table2;
    $tree->{$table2}
      = {position => $i, parent => $table1, join => $join_clause};
  }
  
  # Search need tables
  my $need_tables = {};
  $self->_need_tables($tree, $need_tables, $join_tables);
  my @need_tables = sort { $tree->{$a}{position} <=> $tree->{$b}{position} }
    keys %$need_tables;
  
  # Add join clause
  $$sql .= $tree->{$_}{join} . ' ' for @need_tables;
}

sub _quote {
  my $self = shift;
  return $self->quote || '';
}

sub _remove_duplicate_table {
  my ($self, $tables, $main_table) = @_;
  
  # Remove duplicate table
  my %tables = map {defined $_ ? ($_ => 1) : ()} @$tables;
  delete $tables{$main_table} if $main_table;
  
  my $new_tables = [keys %tables, $main_table ? $main_table : ()];
  if (my $q = $self->_quote) {
    $q = quotemeta($q);
    $_ =~ s/[$q]//g for @$new_tables;
  }

  return $new_tables;
}

sub _search_tables {
  my ($self, $source) = @_;
  
  # Search tables
  my $tables = [];
  my ($q, $p) = $self->_qp(quotemeta => 1);
  $source =~ s/$q//g;
  $source =~ s/$p//g;
  my $c = $self->safety_character;
  
  while ($source =~ /((?:[$c]+?\.[$c]+?)|(?:[$c]+?))\.[$c]+/g) {
    push @$tables, $1;
  }
  return $tables;
}

sub _where_clause_and_param {
  my ($self, $where) = @_;
  
  $where ||= {};
  my $w = {};
  
  if (ref $where eq 'HASH') {
    my $safety_character = $self->safety_character;
    
    my $clause = [];
    my $column_join = '';
    for my $column (sort keys %$where) {
      
      confess qq{"$column" is not safety column name in where clause} . _subname
        unless $column =~ /^[$safety_character\.]+$/;
      
      $column_join .= $column;
      my $table;
      my $c;
      if ($column =~ /(?:(.*)\.)?(.*)/) {
        $table = $1;
        $c = $2;
      }
      
      my $table_quote;
      $table_quote = $self->_tq($table) if defined $table;
      my $column_quote = $self->q($c);
      $column_quote = $table_quote . '.' . $column_quote
        if defined $table_quote;
      if (ref $where->{$column} eq 'ARRAY') {
        my $c = join(', ', (":$column") x @{$where->{$column}});
        if (@{$where->{$column}}) {
          push @$clause, "$column_quote in ( $c )";
        }
        else { push @$clause, '1 <> 1' }
      }
      else { push @$clause, "$column_quote = :$column" }
    }
    
    $w->{clause} = @$clause ? "where ( " . join(' and ', @$clause) . " ) " : '' ;
    $w->{param} = $where;
  }  
  elsif (ref $where) {
    my $obj;

    if (ref $where eq 'DBIx::Custom::Where') { $obj = $where }
    elsif (ref $where eq 'ARRAY') {
      $obj = $self->where(clause => $where->[0], param => $where->[1], join => $where->[2]);
    }
    
    # Check where argument
    confess qq{"where" must be hash reference or DBIx::Custom::Where object}
        . qq{or array reference, which contains where clause and parameter}
        . _subname
      unless ref $obj eq 'DBIx::Custom::Where';

    $w->{clause} = $obj->to_string;
    $w->{param} = $obj->param;
    $w->{join} = $obj->{join};
  }
  elsif ($where) {
    $w->{clause} = "where $where";
  }
  
  return $w;
}

# DEPRECATED
our $AUTOLOAD;
sub AUTOLOAD {
  my $self = shift;
  
  _deprecate('0.39', "DBIx::Custom AUTOLOAD feature is DEPRECATED!");
  
  # Method name
  my ($package, $mname) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

  # Call method
  $self->{_methods} ||= {};
  if (my $method = $self->{_methods}->{$mname}) {
    return $self->$method(@_)
  }
  elsif ($self->{dbh} && (my $dbh_method = $self->dbh->can($mname))) {
    $self->dbh->$dbh_method(@_);
  }
  else {
    confess qq{Can't locate object method "$mname" via "$package" }
      . _subname;
  }
}
sub DESTROY {}

# DEPRECATED
sub helper {
  my $self = shift;
  
  _deprecate('0.39', "DBIx::Custom::helper method is DEPRECATED!");
  
  # Register method
  my $methods = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  $self->{_methods} = {%{$self->{_methods} || {}}, %$methods};
  
  return $self;
}

# DEPRECATED
sub update_or_insert {

  _deprecate('0.39', "DBIx::Custom::update_or_insert method is DEPRECATED!");

  my ($self, $param, %opt) = @_;
  confess "update_or_insert method need primary_key and id option "
    unless defined $opt{id} && defined $opt{primary_key};
  my $statement_opt = $opt{option} || {};

  my $rows = $self->select(%opt, %{$statement_opt->{select} || {}})->all;
  if (@$rows == 0) {
    return $self->insert($param, %opt, %{$statement_opt->{insert} || {}});
  }
  elsif (@$rows == 1) {
    return 0 unless keys %$param;
    return $self->update($param, %opt, %{$statement_opt->{update} || {}});
  }
  else { confess "selected row must be one " . _subname }
}

# DEPRECATED
sub count {
  _deprecate('0.39', "DBIx::Custom::count method is DEPRECATED!");
  shift->select(column => 'count(*)', @_)->fetch_one->[0]
}

1;

=head1 NAME

DBIx::Custom - DBI extension to execute insert, update, delete, and select easily

=head1 SYNOPSIS

  use DBIx::Custom;
  
  # Connect
  my $dbi = DBIx::Custom->connect(
    "dbi:mysql:database=dbname",
    'ken',
    '!LFKD%$&',
    {mysql_enable_utf8 => 1}
  );
  
  # Create model
  $dbi->create_model('book');
  
  # Insert 
  $dbi->model('book')->insert({title => 'Perl', author => 'Ken'});
  
  # Update 
  $dbi->model('book')->update({title => 'Perl', author => 'Ken'}, where  => {id => 5});
  
  # Delete
  $dbi->model('book')->delete(where => {author => 'Ken'});
  
  # Select
  my $result = $dbi->model('book')->select(['title', 'author'], where  => {author => 'Ken'});
  
  # Select, more complex
  #   select book.title as book.title,
  #     book.author as book.author,
  #     comnapy.name as company.name
  #   form book
  #     left outer join company on book.company_id = company.id
  #   where book.author = ?
  #   order by id limit 0, 5
  my $result = $dbi->model('book')->select(
    [
      {book => [qw/title author/]},
      {company => ['name']}
    ],
    where  => {'book.author' => 'Ken'},
    join => ['left outer join company on book.company_id = company.id'],
    append => 'order by id limit 0, 5'
  );
  
  # Get all rows or only one row
  my $rows = $result->all;
  my $row = $result->one;
  
  # Execute SQL with named place holder
  my $result = $dbi->execute(
    "select id from book where author = :author and title like :title",
    {author => 'ken', title => '%Perl%'}
  );
  
=head1 DESCRIPTION

L<DBIx::Custom> is L<DBI> wrapper module to execute SQL easily.
This module have the following features.

=over 4

=item *

Execute C<insert>, C<update>, C<delete>, or C<select> statement easily

=item *

Create C<where> clause flexibly

=item *

Named place holder support

=item *

Model support

=item *

Connection manager support

=item *

Choice your favorite relational database management system,
C<MySQL>, C<SQLite>, C<PostgreSQL>, C<Oracle>,
C<Microsoft SQL Server>, C<Microsoft Access>, C<DB2> or anything, 

=item *

Filtering by data type or column name

=item *

Create C<order by> clause flexibly

=back

=head1 WEB SITE

L<DBIx::Custom - Perl O/R Mapper|http://dbix-custom.hateblo.jp>

=head1 DOCUMENTS

L<DBIx::Custom Documents|https://github.com/yuki-kimoto/DBIx-Custom/wiki>

L<DBIx::Custom API reference|http://search.cpan.org/~kimoto/DBIx-Custom/>

=head1 ATTRIBUTES

=head2 connector

  my $connector = $dbi->connector;
  $dbi = $dbi->connector($connector);

Connection manager object. if C<connector> is set, you can get C<dbh>
through connection manager. Conection manager object must have C<dbh> method.

This is L<DBIx::Connector> example. Please pass
C<default_option> to L<DBIx::Connector> C<new> method.

  my $connector = DBIx::Connector->new(
    "dbi:mysql:database=$database",
    $user,
    $password,
    DBIx::Custom->new->default_option
  );
  
  my $dbi = DBIx::Custom->connect(connector => $connector);

If C<connector> is set to 1 when connect method is called,
L<DBIx::Connector> is automatically set to C<connector>

  my $dbi = DBIx::Custom->connect(
    dsn => $dsn, user => $user, password => $password, connector => 1);
  
  my $connector = $dbi->connector; # DBIx::Connector

Note that L<DBIx::Connector> must be installed.

=head2 dsn

  my $dsn = $dbi->dsn;
  $dbi = $dbi->dsn("DBI:mysql:database=dbname");

Data source name, used when C<connect> method is executed.

=head2 default_option

  my $default_option = $dbi->default_option;
  $dbi = $dbi->default_option($default_option);

L<DBI> default option, used when C<connect> method is executed,
default to the following values.

  {
    RaiseError => 1,
    PrintError => 0,
    AutoCommit => 1,
  }

=head2 exclude_table

  my $exclude_table = $dbi->exclude_table;
  $dbi = $dbi->exclude_table(qr/pg_/);

Excluded table regex.
C<each_column>, C<each_table>, C<type_rule>

=head2 filters

  my $filters = $dbi->filters;
  $dbi = $dbi->filters(\%filters);

Filters, registered by C<register_filter> method.

=head2 last_sql

  my $last_sql = $dbi->last_sql;
  $dbi = $dbi->last_sql($last_sql);

Get last succeeded SQL executed by C<execute> method.

=head2 now

  my $now = $dbi->now;
  $dbi = $dbi->now($now);

Code reference which return current time, default to the following code reference.

  sub {
    my ($sec, $min, $hour, $mday, $mon, $year) = localtime;
    $mon++;
    $year += 1900;
    return sprintf("%04d-%02d-%02d %02d:%02d:%02d");
  }

This return the time like C<2011-10-14 05:05:27>.

This is used by C<insert> method's C<ctime> option and C<mtime> option,
and C<update> method's C<mtime> option.

=head2 models

  my $models = $dbi->models;
  $dbi = $dbi->models(\%models);

Models, included by C<include_model> method.

=head2 mytable_symbol

Symbol to specify own columns in select method column option, default to '__MY__'.

  $dbi->table('book')->select({__MY__ => '*'});

=head2 option

  my $option = $dbi->option;
  $dbi = $dbi->option($option);

L<DBI> option, used when C<connect> method is executed.
Each value in option override the value of C<default_option>.

=head2 password

  my $password = $dbi->password;
  $dbi = $dbi->password('lkj&le`@s');

Password, used when C<connect> method is executed.

=head2 quote

  my quote = $dbi->quote;
  $dbi = $dbi->quote('"');

Reserved word quote.
Default to double quote '"' except for mysql.
In mysql, default to back quote '`'

You can set quote pair.

  $dbi->quote('[]');

=head2 result_class

  my $result_class = $dbi->result_class;
  $dbi = $dbi->result_class('DBIx::Custom::Result');

Result class, default to L<DBIx::Custom::Result>.

=head2 safety_character

  my $safety_character = $dbi->safety_character;
  $dbi = $dbi->safety_character($character);

Regex of safety character for table and column name, default to 'a-zA-Z_'.
Note that you don't have to specify like '[a-zA-Z_]'.

=head2 separator

  my $separator = $dbi->separator;
  $dbi = $dbi->separator('-');

Separator which join table name and column name.
This have effect to C<column> and C<mycolumn> method,
and C<select> method's column option.

Default to C<.>.

=head2 user

  my $user = $dbi->user;
  $dbi = $dbi->user('Ken');

User name, used when C<connect> method is executed.

=head2 user_column_info

  my $user_column_info = $dbi->user_column_info;
  $dbi = $dbi->user_column_info($user_column_info);

You can set the date like the following one.

  [
    {table => 'book', column => 'title', info => {...}},
    {table => 'author', column => 'name', info => {...}}
  ]

Usually, you set return value of C<get_column_info>.

  my $user_column_info
    = $dbi->get_column_info(exclude_table => qr/^system/);
  $dbi->user_column_info($user_column_info);

If C<user_column_info> is set, C<each_column> use C<user_column_info>
to find column info. this is very fast.

=head2 user_table_info

  my $user_table_info = $dbi->user_table_info;
  $dbi = $dbi->user_table_info($user_table_info);

You can set the following data.

  [
    {table => 'book', info => {...}},
    {table => 'author', info => {...}}
  ]

Usually, you can set return value of C<get_table_info>.

  my $user_table_info = $dbi->get_table_info(exclude => qr/^system/);
  $dbi->user_table_info($user_table_info);

If C<user_table_info> is set, C<each_table> use C<user_table_info>
to find table info.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and use all methods of L<DBI>
and implements the following new ones.

=head2 available_datatype

  print $dbi->available_datatype;

Get available data types. You can use these data types
in C<type rule>'s C<from1> and C<from2> section.

=head2 available_typename

  print $dbi->available_typename;

Get available type names. You can use these type names in
C<type_rule>'s C<into1> and C<into2> section.

=head2 assign_clause

  my $assign_clause = $dbi->assign_clause({title => 'a', age => 2});

Create assign clause

  title = :title, author = :author

This is used to create update clause.

  "update book set " . $dbi->assign_clause({title => 'a', age => 2});

=head2 column

  my $column = $dbi->column(book => ['author', 'title']);

Create column clause. The following column clause is created.

  book.author as "book.author",
  book.title as "book.title"

You can change separator by C<separator> attribute.

  # Separator is hyphen
  $dbi->separator('-');
  
  book.author as "book-author",
  book.title as "book-title"
  
=head2 connect
  
  # DBI compatible arguments
  my $dbi = DBIx::Custom->connect(
    "dbi:mysql:database=dbname",
    'ken',
    '!LFKD%$&',
    {mysql_enable_utf8 => 1}
  );
  
  # pass DBIx::Custom attributes
  my $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=dbname",
    user => 'ken',
    password => '!LFKD%$&',
    option => {mysql_enable_utf8 => 1}
  );

Connect to the database and create a new L<DBIx::Custom> object.

L<DBIx::Custom> is a wrapper of L<DBI>.
C<AutoCommit> and C<RaiseError> options are true, 
and C<PrintError> option is false by default.

=head2 create_model
  
  $dbi->create_model('book');
  $dbi->create_model(
    'book',
    join => [
      'inner join company on book.comparny_id = company.id'
    ]
  );
  $dbi->create_model(
    table => 'book',
    join => [
      'inner join company on book.comparny_id = company.id'
    ],
  );

Create L<DBIx::Custom::Model> object and initialize model.
Model columns attribute is automatically set.
You can use this model by using C<model> method.

  $dbi->model('book')->select(...);

You can use model name which different from table name

  $dbi->create_model(name => 'book1', table => 'book');
  $dbi->model('book1')->select(...);

  $dbi->create_model(
    table => 'book',
    join => [
      'inner join company on book.comparny_id = company.id'
    ],
  );

C<column_name_lc> option change column names to lower case.

  $dbi->create_model(
    table => 'book',
    join => [
      'inner join company on book.comparny_id = company.id'
    ],
    column_name_lc => 1,
  );

=head2 dbh

  my $dbh = $dbi->dbh;

Get L<DBI> database handle. if C<connector> is set, you can get
database handle through C<connector> object.

=head2 delete

  $dbi->delete(table => 'book', where => {title => 'Perl'});

Execute delete statement.

The following options are available.

B<OPTIONS>

C<delete> method use all of C<execute> method's options,
and use the following new ones.

=over 4

=item prefix

  prefix => 'some'

prefix before table name section.

  delete some from book

=item table

  table => 'book'

Table name.

=item where

Same as C<select> method's C<where> option.

=back

=head2 delete_all

  $dbi->delete_all(table => $table);

Execute delete statement for all rows.
Options is same as C<delete>.

=head2 each_column

  $dbi->each_column(
    sub {
      my ($dbi, $table, $column, $column_info) = @_;
      
      my $type = $column_info->{TYPE_NAME};
      
      if ($type eq 'DATE') {
          # ...
      }
    }
  );

Iterate all column informations in database.
Argument is callback which is executed when one column is found.
Callback receive four arguments. C<DBIx::Custom object>, C<table name>,
C<column name>, and C<column information>.

If C<user_column_info> is set, C<each_column> method use C<user_column_info>
information, you can improve the performance of C<each_column> in
the following way.

  my $column_infos = $dbi->get_column_info(exclude_table => qr/^system_/);
  $dbi->user_column_info($column_info);
  $dbi->each_column(sub { ... });

=head2 each_table

  $dbi->each_table(
    sub {
      my ($dbi, $table, $table_info) = @_;
      
      my $table_name = $table_info->{TABLE_NAME};
    }
  );

Iterate all table information from in database.
Argument is callback which is executed when one table is found.
Callback receive three arguments, C<DBIx::Custom object>, C<table name>,
C<table information>.

If C<user_table_info> is set, C<each_table> method use C<user_table_info>
information, you can improve the performance of C<each_table> in
the following way.

  my $table_infos = $dbi->get_table_info(exclude => qr/^system_/);
  $dbi->user_table_info($table_info);
  $dbi->each_table(sub { ... });

=head2 execute

  my $result = $dbi->execute(
    "select * from book where title = :title and author like :author",
    {title => 'Perl', author => '%Ken%'}
  );

  my $result = $dbi->execute(
    "select * from book where title = :book.title and author like :book.author",
    {'book.title' => 'Perl', 'book.author' => '%Ken%'}
  );

Execute SQL. SQL can contain column parameter such as :author and :title.
You can append table name to column name such as :book.title and :book.author.
Second argument is data, embedded into column parameter.
Return value is L<DBIx::Custom::Result> object when select statement is executed,
or the count of affected rows when insert, update, delete statement is executed.

Named placeholder such as C<:title> is replaced by placeholder C<?>.
  
  # Original
  select * from book where title = :title and author like :author
  
  # Replaced
  select * from where title = ? and author like ?;

You can specify operator with named placeholder
by C<name{operator}> syntax.

  # Original
  select * from book where :title{=} and :author{like}
  
  # Replaced
  select * from where title = ? and author like ?;

Note that colons in time format such as 12:13:15 is an exception,
it is not parsed as named placeholder.
If you want to use colon generally, you must escape it by C<\\>

  select * from where title = "aa\\:bb";

B<OPTIONS>

The following options are available.

=over 4

=item after_build_sql 

You can filter sql after the sql is build.

  after_build_sql => $code_ref

The following one is one example.

  $dbi->select(
    table => 'book',
    column => 'distinct(name)',
    after_build_sql => sub {
      "select count(*) from ($_[0]) as t1"
    }
  );

The following SQL is executed.

  select count(*) from (select distinct(name) from book) as t1;

=item append

  append => 'order by name'

Append some statement after SQL.

=item bind_type

Specify database bind data type.
  
  bind_type => {image => DBI::SQL_BLOB}
  bind_type => [image => DBI::SQL_BLOB]
  bind_type => [[qw/image audio/] => DBI::SQL_BLOB]

This is used to bind parameter by C<bind_param> of statement handle.

  $sth->bind_param($pos, $value, DBI::SQL_BLOB);

=item filter
  
  filter => {
    title  => sub { uc $_[0] }
    author => sub { uc $_[0] }
  }

  # Filter name
  filter => {
    title  => 'upper_case',
    author => 'upper_case'
  }
      
  # At once
  filter => [
    [qw/title author/]  => sub { uc $_[0] }
  ]

Filter. You can set subroutine or filter name
registered by C<register_filter>.
This filter is executed before data is saved into database.
and before type rule filter is executed.

=item reuse
  
  reuse => $hash_ref

Reuse statement handle in same SQL.
  
  my $reuse = {};
  $dbi->execute($sql, $param, reuse => $reuse);

This will improved performance when you want to execute same sql repeatedly.

=item table
  
  table => 'author'

If you want to omit table name in column name
and enable C<into1> and C<into2> type filter,
You must set C<table> option.

  $dbi->execute("select * from book where title = :title and author = :author",
    {title => 'Perl', author => 'Ken', table => 'book');

  # Same
  $dbi->execute(
    "select * from book where title = :book.title and author = :book.author",
    {title => 'Perl', author => 'Ken');

=item table_alias

  table_alias => {worker => 'user'} # {ALIAS => TABLE}

Table alias. Key is alias table name, value is real table name, .
If you set C<table_alias>, you can enable C<into1> and C<into2> type rule
on alias table name.

=item type_rule_off

  type_rule_off => 1

Turn C<into1> and C<into2> type rule off.

=item type_rule1_off

  type_rule1_off => 1

Turn C<into1> type rule off.

=item type_rule2_off

  type_rule2_off => 1

Turn C<into2> type rule off.

=item prepare_attr

  prepare_attr => {mysql_use_result => 1}

Statemend handle attributes,
this is L<DBI>'s C<prepare> method second argument.

=head2 get_column_info

  my $column_infos = $dbi->get_column_info(exclude_table => qr/^system_/);

get column information except for one which match C<exclude_table> pattern.

  [
    {table => 'book', column => 'title', info => {...}},
    {table => 'author', column => 'name' info => {...}}
  ]

=head2 get_table_info

  my $table_infos = $dbi->get_table_info(exclude => qr/^system_/);

get table information except for one which match C<exclude> pattern.

  [
    {table => 'book', info => {...}},
    {table => 'author', info => {...}}
  ]

You can set this value to C<user_table_info>.

=head2 insert

  $dbi->insert({title => 'Perl', author => 'Ken'}, table  => 'book');

Execute insert statement. First argument is row data. Return value is
affected row count.

If you want to set constant value to row data, use scalar reference
as parameter value.

  {date => \"NOW()"}

You can pass multiple parameters, this is very fast.

  $dbi->insert(
    [
      {title => 'Perl', author => 'Ken'},
      {title => 'Ruby', author => 'Tom'}
    ],
    table  => 'book'
  );

In multiple insert, you can't use C<id> option.
and only first parameter is used to create sql.

B<options>

C<insert> method use all of C<execute> method's options,
and use the following new ones.

=over 4

=item bulk_insert

  bulk_insert => 1

bulk insert is executed if database support bulk insert and 
multiple parameters is passed to C<insert>.
The SQL like the following one is executed.

  insert into book (id, title) values (?, ?), (?, ?);

=item ctime

  ctime => 'created_time'

Created time column name. time when row is created is set to the column.
default time format is "YYYY-mm-dd HH:MM:SS", which can be changed by
C<now> attribute.

=item prefix

  prefix => 'or replace'

prefix before table name section

  insert or replace into book

=item table

  table => 'book'

Table name.

=item mtime

This option is same as C<update> method C<mtime> option.

=item wrap

  wrap => {price => sub { "max($_[0])" }}

placeholder wrapped string.

If the following statement

  $dbi->insert({price => 100}, table => 'book',
    {price => sub { "$_[0] + 5" }});

is executed, the following SQL is executed.

  insert into book price values ( ? + 5 );

=back

=over 4

=head2 include_model

  $dbi->include_model('MyModel');

Include models from specified namespace,
the following layout is needed to include models.

  lib / MyModel.pm
      / MyModel / book.pm
                / company.pm

Name space module, extending L<DBIx::Custom::Model>.

B<MyModel.pm>

  package MyModel;
  use DBIx::Custom::Model -base;
  
  1;

Model modules, extending name space module.

B<MyModel/book.pm>

  package MyModel::book;
  use MyModel -base;
  
  1;

B<MyModel/company.pm>

  package MyModel::company;
  use MyModel -base;
  
  1;
  
MyModel::book and MyModel::company is included by C<include_model>.

You can get model object by C<model>.

  my $book_model = $dbi->model('book');
  my $company_model = $dbi->model('company');

You can include full-qualified table name like C<main.book>

  lib / MyModel.pm
      / MyModel / main / book.pm
                       / company.pm

  my $main_book = $self->model('main.book');

See L<DBIx::Custom::Model> to know model features.

=head2 like_value

  my $like_value = $dbi->like_value

Code reference which return a value for the like value.

  sub { "%$_[0]%" }

=head2 mapper

  my $mapper = $dbi->mapper(param => $param);

Create a new L<DBIx::Custom::Mapper> object.

=head2 merge_param

  my $param = $dbi->merge_param({key1 => 1}, {key1 => 1, key2 => 2});

Merge parameters. The following new parameter is created.

  {key1 => [1, 1], key2 => 2}

If same keys contains, the value is converted to array reference.

=head2 model

  my $model = $dbi->model('book');

Get a L<DBIx::Custom::Model> object
create by C<create_model> or C<include_model>

=head2 mycolumn

  my $column = $dbi->mycolumn(book => ['author', 'title']);

Create column clause for myself. The following column clause is created.

  book.author as author,
  book.title as title

=head2 new

  my $dbi = DBIx::Custom->new(
    dsn => "dbi:mysql:database=dbname",
    user => 'ken',
    password => '!LFKD%$&',
    option => {mysql_enable_utf8 => 1}
  );

Create a new L<DBIx::Custom> object.

=head2 not_exists

  my $not_exists = $dbi->not_exists;

DBIx::Custom::NotExists object, indicating the column is not exists.
This is used in C<param> of L<DBIx::Custom::Where> .

=head2 order

  my $order = $dbi->order;

Create a new L<DBIx::Custom::Order> object.

=head2 q

  my $quooted = $dbi->q("title");

Quote string by value of C<quote>.

=head2 register_filter

  $dbi->register_filter(
    # Time::Piece object to database DATE format
    tp_to_date => sub {
      my $tp = shift;
      return $tp->strftime('%Y-%m-%d');
    },
    # database DATE format to Time::Piece object
    date_to_tp => sub {
      my $date = shift;
      return Time::Piece->strptime($date, '%Y-%m-%d');
    }
  );
  
Register filters, used by C<filter> option of many methods.

=head2 select

  my $result = $dbi->select(
    column => ['author', 'title'],
    table  => 'book',
    where  => {author => 'Ken'},
  );
  
Execute select statement.

You can pass odd number arguments. first argument is C<column>.

  my $result = $dbi->select(['author', 'title'], table => 'book');

B<OPTIONS>

C<select> method use all of C<execute> method's options,
and use the following new ones.

=over 4

=item column
  
  column => 'author'
  column => ['author', 'title']

Column clause.
  
if C<column> is not specified, '*' is set.

  column => '*'

You can specify hash of array reference.

  column => [
    {book => [qw/author title/]},
    {person => [qw/name age/]}
  ]

This is expanded to the following one by using C<colomn> method.

  book.author as "book.author",
  book.title as "book.title",
  person.name as "person.name",
  person.age as "person.age"

You can specify own column by C<__MY__>.

  column => [
    {__MY__ => [qw/author title/]},
  ]

This is expanded to the following one by using C<mycolomn> method.

  book.author as "author",
  book.title as "title",

C<__MY__> can be changed by C<mytable_symbol> attribute.

=item param

  param => {'table2.key3' => 5}

Parameter shown before where clause.
  
For example, if you want to contain named placeholder in join clause, 
you can pass parameter by C<param> option.

  join  => ['inner join (select * from table2 where table2.key3 = :table2.key3)' . 
            ' as table2 on table1.key1 = table2.key1']

=item prefix

  prefix => 'SQL_CALC_FOUND_ROWS'

Prefix of column clause

  select SQL_CALC_FOUND_ROWS title, author from book;

=item join

  join => [
    'left outer join company on book.company_id = company_id',
    'left outer join location on company.location_id = location.id'
  ]
      
Join clause. If column clause or where clause contain table name like "company.name",
join clauses needed when SQL is created is used automatically.

  $dbi->select(
    table => 'book',
    column => ['company.location_id as location_id'],
    where => {'company.name' => 'Orange'},
    join => [
      'left outer join company on book.company_id = company.id',
      'left outer join location on company.location_id = location.id'
    ]
  );

In above select, column and where clause contain "company" table,
the following SQL is created

  select company.location_id as location_id
  from book
    left outer join company on book.company_id = company.id
  where company.name = ?;

You can specify two table by yourself. This is useful when join parser can't parse
the join clause correctly.

  $dbi->select(
    table => 'book',
    column => ['company.location_id as location_id'],
    where => {'company.name' => 'Orange'},
    join => [
      {
        clause => 'left outer join location on company.location_id = location.id',
        table => ['company', 'location']
      }
    ]
  );

=item table

  table => 'book'

Table name.

=item where
  
  # (1) Hash reference
  where => {author => 'Ken', 'title' => ['Perl', 'Ruby']}
  # -> where author = 'Ken' and title in ('Perl', 'Ruby')
  
  # (2) DBIx::Custom::Where object
  where => $dbi->where(
    clause => ['and', ':author{=}', ':title{like}'],
    param  => {author => 'Ken', title => '%Perl%'}
  )
  # -> where author = 'Ken' and title like '%Perl%'
  
  # (3) Array reference[where clause, parameters, join(optional)]
  where => [
    ['and', ':author{=}', ':title{like}'],
    {author => 'Ken', title => '%Perl%'},
    ["left outer join table2 on table1.key1 = table2.key1"]
  ]
  # -> where author = 'Ken' and title like '%Perl%'
  
  # (4) Array reference[String, Hash reference]
  where => [
    ':author{=} and :title{like}',
    {author => 'Ken', title => '%Perl%'}
  ]
  #  -> where author = 'Ken' and title like '%Perl%'
  
  # (5) String
  where => 'title is null'
  #  -> where title is null

Where clause.
See also L<DBIx::Custom::Where> to know how to create where clause.
  
=back

=head2 type_rule

  $dbi->type_rule(
    into1 => {
      date => sub { ... },
      datetime => sub { ... }
    },
    into2 => {
      date => sub { ... },
      datetime => sub { ... }
    },
    from1 => {
      # DATE
      9 => sub { ... },
      # DATETIME or TIMESTAMP
      11 => sub { ... },
    }
    from2 => {
      # DATE
      9 => sub { ... },
      # DATETIME or TIMESTAMP
      11 => sub { ... },
    }
  );

Filtering rule when data is send into and get from database.
This has a little complex problem.

In C<into1> and C<into2> you can specify
type name as same as type name defined
by create table, such as C<DATETIME> or C<DATE>.

Note that type name and data type don't contain upper case.
If these contain upper case character, you convert it to lower case.

C<into2> is executed after C<into1>.

Type rule of C<into1> and C<into2> is enabled on the following
column name.

=over 4

=item 1. column name

  issue_date
  issue_datetime

This need C<table> option in each method.

=item 2. table name and column name, separator is dot

  book.issue_date
  book.issue_datetime

=back

You get all type name used in database by C<available_typename>.

  print $dbi->available_typename;

In C<from1> and C<from2> you specify data type, not type name.
C<from2> is executed after C<from1>.
You get all data type by C<available_datatype>.

  print $dbi->available_datatype;

You can also specify multiple types at once.

  $dbi->type_rule(
    into1 => [
      [qw/DATE DATETIME/] => sub { ... },
    ],
  );

=head2 update

  $dbi->update({title => 'Perl'}, table  => 'book', where  => {id => 4});

Execute update statement. First argument is update row data.

If you want to set constant value to row data, use scalar reference
as parameter value.

  {date => \"NOW()"}

B<OPTIONS>

C<update> method use all of C<execute> method's options,
and use the following new ones.

=over 4

=item prefix

  prefix => 'or replace'

prefix before table name section

  update or replace book

=item table

  table => 'book'

Table name.

=item where

Same as C<select> method's C<where> option.

=item wrap

  wrap => {price => sub { "max($_[0])" }}

placeholder wrapped string.

If the following statement

  $dbi->update({price => 100}, table => 'book',
    {price => sub { "$_[0] + 5" }});

is executed, the following SQL is executed.

  update book set price =  ? + 5;

=item mtime

  mtime => 'modified_time'

Modified time column name. time row is updated is set to the column.
default time format is C<YYYY-mm-dd HH:MM:SS>, which can be changed by
C<now> attribute.

=back

=head2 update_all

  $dbi->update_all({title => 'Perl'}, table => 'book', );

Execute update statement for all rows.
Options is same as C<update> method.

=over 4

=item option

  option => {
    select => {
      append => '...'
    },
    insert => {
      prefix => '...'
    },
    update => {
      filter => {}
    }
  }

If you want to pass option to each method,
you can use C<option> option.

=over 4

=item select_option

  select_option => {append => 'for update'}

select method option,
select method is used to check the row is already exists.

=head2 show_datatype

  $dbi->show_datatype($table);

Show data type of the columns of specified table.

  book
  title: 5
  issue_date: 91

This data type is used in C<type_rule>'s C<from1> and C<from2>.

=head2 show_tables

  $dbi->show_tables;

Show tables.

=head2 show_typename

  $dbi->show_typename($table);

Show type name of the columns of specified table.

  book
  title: varchar
  issue_date: date

This type name is used in C<type_rule>'s C<into1> and C<into2>.

=head2 values_clause

  my $values_clause = $dbi->values_clause({title => 'a', age => 2});

Create values clause.

  (title, author) values (title = :title, age = :age);

You can use this in insert statement.

  my $insert_sql = "insert into book $values_clause";

=head2 where

  my $where = $dbi->where;
  $where->clause(['and', 'title = :title', 'author = :author']);
  $where->param({title => 'Perl', author => 'Ken'});
  $where->join(['left join author on book.author = author.id]);

Create a new L<DBIx::Custom::Where> object.
See L<DBIx::Custom::Where> to know how to create where clause.

=head1 ENVIRONMENTAL VARIABLES

=head2 DBIX_CUSTOM_DEBUG

If environment variable C<DBIX_CUSTOM_DEBUG> is set to true,
executed SQL and bind values are printed to STDERR.

=head2 DBIX_CUSTOM_DEBUG_ENCODING

DEBUG output encoding. Default to UTF-8.

=head2 DBIX_CUSTOM_SUPPRESS_DEPRECATION

  $ENV{DBIX_CUSTOM_SUPPRESS_DEPRECATION} = '0.25';

Suppress deprecation warnings before specified version.

=head1 DEPRECATED FUNCTIONALITY

L<DBIx::Custom>

  # Methods
  DBIx::Custom AUTOLOAD feature # will be removed at 2022/5/1
  DBIx::Custom::helper method # will be removed at 2022/5/1
  DBIx::Custom::update_or_insert method is DEPRECATED! # will be removed at 2022/5/1
  DBIx::Custom::count method # will be removed at 2022/5/1
  DBIx::Custom::select,update,delete method's primary_key option is DEPRECATED! # will be removed at 2022/5/1
  DBIx::Custom::select,update,delete method's id option is DEPRECATED! # will be removed at 2022/5/1
  DBIx::Custom::setup method is DEPRECATED! # will be removed at 2022/5/1

L<DBIx::Custom::Result>
  
  # Options
  kv method's multi option (from 0.28) # will be removed at 2018/3/1

L<DBIx::Custom::Model>

  DBIx::Custom::Model AUTOLOAD feature # will be removed at 2022/5/1
  DBIx::Custom::Model::helper method is DEPRECATED! # will be removed at 2022/5/1
  DBIx::Custom::Model::update_or_insert method is DEPRECATED! # will be removed at 2022/5/1
  DBIx::Custom::Model::count method # will be removed at 2022/5/1
  DBIx::Custom::Model::primary_key attribute is DEPRECATED! # will be removed at 2022/5/1

=head1 BACKWARDS COMPATIBILITY POLICY

If a feature is DEPRECATED, you can know it by DEPRECATED warnings.
DEPRECATED feature is removed after C<five years>,
but if at least one person use the feature and tell me that thing
I extend one year each time he tell me it.

DEPRECATION warnings can be suppressed by C<DBIX_CUSTOM_SUPPRESS_DEPRECATION>
environment variable.

EXPERIMENTAL features will be changed or deleted without warnings.

=head1 BUGS

Please tell me bugs if you find bug.

C<< <kimoto.yuki at gmail.com> >>

L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009-2019 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
