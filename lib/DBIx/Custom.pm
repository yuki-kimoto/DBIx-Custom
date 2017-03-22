use 5.008007;
package DBIx::Custom;
use Object::Simple -base;

our $VERSION = '0.39';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Where;
use DBIx::Custom::Model;
use DBIx::Custom::Order;
use DBIx::Custom::Util qw/_array_to_hash _subname _deprecate/;
use DBIx::Custom::Mapper;
use DBIx::Custom::NotExists;
use Encode qw/encode encode_utf8 decode_utf8/;
use Scalar::Util qw/weaken/;

has [qw/connector dsn default_schema password quote user exclude_table user_table_info
     user_column_info safety_character/];
has async_conf => sub { {} };
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
    croak qq{Can't locate object method "$mname" via "$package" }
      . _subname;
  }
}

sub assign_clause {
  my ($self, $param, $opts) = @_;
  
  my $wrap = $opts->{wrap} || {};
  my ($q, $p) = $self->_qp;
  
  # Assign clause (performance is important)
  join(
    ', ',
    map {
      ref $param->{$_} eq 'SCALAR' ? "$q$_$p = " . ${$param->{$_}}
      : $wrap->{$_} ? "$q$_$p = " . $wrap->{$_}->(":$_")
      : "$q$_$p = :$_";
    } sort keys %$param
  );
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
  my $self = ref $_[0] ? shift : shift->new(@_);
  
  my $connector = $self->connector;
  
  if (!ref $connector && $connector) {
    require DBIx::Connector;
    
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

sub count { shift->select(column => 'count(*)', @_)->fetch_one->[0] }

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
      croak "connector must have dbh() method " . _subname
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
  croak qq{delete method where or id option must be specified } . _subname
    if !$opt{where} && !defined $opt{id} && !$opt{allow_delete_all};
  
  # Where
  my $w = $self->_where_clause_and_param($opt{where}, delete $opt{id}, $opt{primary_key}, $opt{table});
  
  # Delete statement
  my $sql = "delete ";
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  $sql .= "from " . $self->_tq($opt{table}) . " $w->{clause} ";
  
  # Execute query
  $opt{statement} = 'delete';
  $self->execute($sql, $w->{param}, %opt);
}

sub delete_all { shift->delete(@_, allow_delete_all => 1) }

sub DESTROY {}

sub create_model {
  my $self = shift;
  
  # Options
  my $opt = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  $opt->{dbi} = $self;
  my $model_class = delete $opt->{model_class} || 'DBIx::Custom::Model';
  my $model_name  = delete $opt->{name};
  my $model_table = delete $opt->{table};
  $model_name ||= $model_table;
  
  # Create model
  my $model = $model_class->new($opt);
  weaken $model->{dbi};
  $model->table($model_table) unless $model->table;
  
  # Set model
  $self->model($model->table, $model);
  
  return $self->model($model->table);
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

sub execute {
  my $self = shift;
  my $sql = shift;

  # Options
  my $params;
  $params = shift if @_ % 2;
  my %opt = @_;
  
  # Async query
  $opt{prepare_attr} = $self->async_conf->{prepare_attr} if $opt{async};
  if ($opt{async} && !$self->{_new_connection}) {
    my $dsn = $self->dsn;
    croak qq/Data source must be specified when "async" option is used/
      unless defined $dsn;
    
    my $user = $self->user;
    my $password = $self->password;
    my $option = $self->option;
    
    my $new_dbi = bless {%$self}, ref $self;
    $new_dbi->connector(undef);
    $new_dbi->{dbh} = DBI->connect($dsn, $user, $password,
      {%{$new_dbi->default_option}, %$option, PrintError => 0, RaiseError => 0});
    
    $new_dbi->{_new_connection} = 1;
    return $new_dbi->execute($sql, defined $params ? ($params) : (), %opt);
  }
  
  # Options
  $params ||= $opt{param} || {};
  my $tables = $opt{table} || [];
  $tables = [$tables] unless ref $tables eq 'ARRAY';
  my $filter = ref $opt{filter} eq 'ARRAY' ?
    _array_to_hash($opt{filter}) : $opt{filter};
  
  # Merge second parameter
  $opt{statement} ||= '';
  $opt{statement} = 'select' if $opt{select};
  $params = [$params] unless ref $params eq 'ARRAY';
  
  # Append
  $sql .= $opt{append} if defined $opt{append};
  
  # Query
  my $query;
  $query = $opt{reuse}->{$sql} if $opt{reuse};
  unless ($query) {
    my $c = $self->{safety_character};
    # Check unsafety keys
    unless ((join('', keys %{$params->[0]}) || '') =~ /^[$c\.]+$/) {
      for my $column (keys %{$params->[0]}) {
        croak qq{"$column" is not safety column name } . _subname
          unless $column =~ /^[$c\.]+$/;
      }
    }
    $query = $self->_create_query($sql, $opt{after_build_sql}, $opt{prepare_attr});
  }
  $query->{statement} = $opt{statement} || '';
  $opt{reuse}->{$sql} = $query if $opt{reuse};
      
  # Save query
  $self->{last_sql} = $query->{sql};
  
  # Tables
  unshift @$tables, @{$query->{tables} || []};
  my $main_table = @{$tables}[-1];
  
  # Type rule
  my $type_filters = {};
  my $type_rule_off = !$self->{_type_rule_is_called} || $opt{type_rule_off};
  unless ($type_rule_off) {
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
  
  # Replace filter name to code
  for my $column (keys %$filter) {
    my $name = $filter->{$column};
    if (!defined $name) {
      $filter->{$column} = undef;
    }
    elsif (ref $name ne 'CODE') {
      croak qq{Filter "$name" is not registered" } . _subname
        unless exists $self->filters->{$name};
      $filter->{$column} = $self->filters->{$name};
    }
  }

  # Execute
  my $sth = $query->{sth};
  my $affected;
  if ((!$query->{duplicate} || $opt{bulk_insert}) && $type_rule_off
    && !keys %$filter
    && !$opt{bind_type} && !$opt{type} && !$ENV{DBIX_CUSTOM_DEBUG})
  {
    eval {
      if ($opt{bulk_insert}) {
        my %count;
        my $param = $params->[0];
        $affected = $sth->execute(map { $param->{$_}->[++$count{$_} - 1] }
          @{$query->{columns}});
      }
      else {
        for my $param (@$params) {
          $affected = $sth->execute(map { $param->{$_} } @{$query->{columns}});
        }
      }
    };
  }
  else {
    for my $param (@$params) {
      # Create bind values
      my ($bind, $bind_types) = $self->_create_bind_values($param, $query->{columns},
        $filter, $type_filters, $opt{bind_type} || $opt{type} || {});

      # Execute
      eval {
        if ($opt{bind_type} || $opt{type}) {
          $sth->bind_param($_ + 1, $bind->[$_],
              $bind_types->[$_] ? $bind_types->[$_] : ())
            for (0 .. @$bind - 1);
          $affected = $sth->execute;
        }
        else { $affected = $sth->execute(@$bind) }

        # DEBUG message
        if ($ENV{DBIX_CUSTOM_DEBUG}) {
          warn "SQL:\n" . $query->{sql} . "\n";
          my @output;
          for my $value (@$bind) {
            $value = 'undef' unless defined $value;
            $value = encode($ENV{DBIX_CUSTOM_DEBUG_ENCODING} || 'UTF-8', $value)
              if utf8::is_utf8($value);
            push @output, $value;
          }
          warn "Bind values: " . join(', ', @output) . "\n\n";
        }
      };
    }
  }
  
  $self->_croak($@, qq{. Following SQL is executed.\n}
    . qq{$query->{sql}\n} . _subname) if $@;
  
  # Affected of insert, update, or delete
  if (!$sth->{NUM_OF_FIELDS} && $opt{statement} ne 'select') {
    # Non-Blocking
    if (my $cb = $opt{async}) {
      require AnyEvent;
      my $watcher;
      $watcher = AnyEvent->io(
        fh => $self->async_conf->{fh}->($self),
        poll => 'w',
        cb => sub {
          my $affected;
          my $driver = $self->_driver;
          if ($driver eq 'mysql') {
            $affected = $sth->mysql_async_result;
          }
          $cb->($self, $affected);
          undef $watcher;
          undef $cb;
          undef $self;
        }
      );
    }
    # Blocking
    else { return $affected }
  }
  # Reulst of select statement
  else {
    
    # Result
    my $result = $self->result_class->new(
      sth => $sth,
      dbi => $self,
      type_rule => {
        from1 => $self->type_rule->{from1},
        from2 => $self->type_rule->{from2}
      },
    );
    
    # Non-Blocking
    if (my $cb = $opt{async}) {
      require AnyEvent;
      my $watcher;
      $watcher = AnyEvent->io(
        fh => $self->async_conf->{fh}->($self),
        poll => 'r',
        cb   => sub {
          my $error;
          my $driver = $self->_driver;
          if ($driver eq 'mysql') {
            $sth->mysql_async_result;
          }
          
          $cb->($self, $result);
          undef $watcher;
          undef $result;
          undef $cb;
          undef $self;
        },
      );
    }
    # Blocking
    else { return $result }
  }
}

sub get_table_info {
  my ($self, %opt) = @_;
  
  my $exclude = delete $opt{exclude};
  croak qq/"$_" is wrong option/ for keys %opt;
  
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
  croak qq/"$_" is wrong option/ for keys %opt;
  
  my $column_info = [];
  $self->each_column(
    sub { push @$column_info, {table => $_[1], column => $_[2], info => $_[3] } },
    exclude_table => $exclude_table
  );
  
  return [
    sort {$a->{table} cmp $b->{table} || $a->{column} cmp $b->{column} }
      @$column_info];
}

sub helper {
  my $self = shift;
  
  _deprecate('0.39', "DBIx::Custom::helper method is DEPRECATED!");
  
  # Register method
  my $methods = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  $self->{_methods} = {%{$self->{_methods} || {}}, %$methods};
  
  return $self;
}

sub insert {
  my $self = shift;
  
  # Options
  my $params = @_ % 2 ? shift : undef;
  my %opt = @_;
  $params ||= {};
  
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
  my $id_param = {};
  if (defined $opt{id} && !$multi) {
    for my $param (@$params) {
      $param = {%$param};
    }
    
    croak "insert id option must be specified with primary_key option"
      unless $opt{primary_key};
    $opt{primary_key} = [$opt{primary_key}] unless ref $opt{primary_key} eq 'ARRAY';
    $opt{id} = [$opt{id}] unless ref $opt{id} eq 'ARRAY';
    for (my $i = 0; $i < @{$opt{primary_key}}; $i++) {
      my $key = $opt{primary_key}->[$i];
      next if exists $params->[0]->{$key};
      $params->[0]->{$key} = $opt{id}->[$i];
    }
  }
  
  # Insert statement
  my $sql = "insert ";
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  $sql .= "into " . $self->_tq($opt{table}) . " ";
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
  $opt{statement} = 'insert';
  $self->execute($sql, $params, %opt);
}

sub include_model {
  my ($self, $name_space, $model_infos) = @_;
  
  # Name space
  $name_space ||= '';
  
  # Get Model information
  unless ($model_infos) {

    # Load name space module
    croak qq{"$name_space" is invalid class name } . _subname
      if $name_space =~ /[^\w:]/;
    eval "use $name_space";
    croak qq{Name space module "$name_space.pm" is needed. $@ } . _subname
      if $@;
    
    # Search model modules
    my $name_space_dir = $name_space;
    $name_space_dir =~ s/::/\//g;
    my $path = $INC{"$name_space_dir.pm"};
    $path =~ s/\.pm$//;
    opendir my $dh, $path
      or croak qq{Can't open directory "$path": $! } . _subname
    my @modules;
    while (my $file = readdir $dh) {
      my $file_abs = "$path/$file";
      if (-d $file_abs) {
        next if $file eq '.' || $file eq '..';
        opendir my $fq_dh, $file_abs
          or croak qq{Can't open directory "$file_abs": $! } . _subname;
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
    croak qq{"$mclass" is invalid class name } . _subname
      if $mclass =~ /[^\w:]/;
    unless ($mclass->can('new')) {
      eval "require $mclass";
      croak "$@ " . _subname if $@;
    }
    
    # Create model
    my $opt = {};
    $opt->{model_class} = $mclass if $mclass;
    $opt->{name}        = $model_name if $model_name;
    $opt->{table}       = $model_table if $model_table;
    $self->create_model($opt);
  }
  
  return $self;
}

sub like_value { sub { "%$_[0]%" } }

sub mapper {
  my $self = shift;
  return DBIx::Custom::Mapper->new(@_);
}

sub merge_param {
  my ($self, @params) = @_;
  
  # Merge parameters
  my $merge = {};
  for my $param (@params) {
    for my $column (keys %$param) {
      my $param_is_array = ref $param->{$column} eq 'ARRAY' ? 1 : 0;
      
      if (exists $merge->{$column}) {
        $merge->{$column} = [$merge->{$column}]
          unless ref $merge->{$column} eq 'ARRAY';
        push @{$merge->{$column}},
          ref $param->{$column} ? @{$param->{$column}} : $param->{$column};
      }
      else { $merge->{$column} = $param->{$column} }
    }
  }
  
  return $merge;
}

sub model {
  my ($self, $name, $model) = @_;
  
  # Set model
  if ($model) {
    $self->models->{$name} = $model;
    return $self;
  }
  
  # Check model existence
  croak qq{Model "$name" is not included } . _subname
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
  my $self = shift->SUPER::new(@_);
  
  # Check attributes
  my @attrs = keys %$self;
  for my $attr (@attrs) {
    croak qq{Invalid attribute: "$attr" } . _subname
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
  $opt{statement} = 'select';
  $opt{column} = $column if defined $column;

  # Options
  my $table_is_empty;
  my $tables = ref $opt{table} eq 'ARRAY' ? $opt{table}
    : defined $opt{table} ? [$opt{table}]
    : [];
  $opt{table} = $tables;
  $table_is_empty = 1 unless @$tables;
  $opt{param} ||= {};
  
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
          $column = $self->mycolumn($tables->[0] => $columns);
        }
        else {
          $column = $self->column($table => $columns);
        }
      }
      unshift @$tables, @{$self->_search_tables($column)}
        unless $table_is_empty;
      $sql .= "$column, ";
    }
    $sql =~ s/, $/ /;
  }
  else { $sql .= '* ' }

  # Execute query without table
  return $self->execute($sql, {}, %opt) if $table_is_empty;

  # Table
  $sql .= 'from ';
  $sql .= $self->_tq($tables->[-1] || '') . ' ';
  $sql =~ s/, $/ /;

  # Add tables in parameter
  unshift @$tables, @{$self->_search_tables(join(' ', keys %{$opt{param}}) || '')};
  
  # Where
  my $w = $self->_where_clause_and_param($opt{where}, delete $opt{id}, $opt{primary_key}, @$tables ? $tables->[-1] : undef);
  $opt{param} = $self->merge_param($opt{param}, $w->{param});
  
  # Add table names in where clause
  unshift @$tables, @{$self->_search_tables($w->{clause})};
  
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
  $self->_push_join(\$sql, $join, $tables) if @$join;
  
  # Add where clause
  $sql .= "$w->{clause} ";
  
  # Execute query
  return $self->execute($sql, %opt);
}

sub setup_model {
  my ($self, %opt) = @_;
  
  # Setup model
  $self->each_column(
    sub {
      my ($self, $table, $column, $column_info) = @_;
      my $schema = $column_info->{TABLE_SCHEM};
      
      my $default_schema = $self->default_schema;
      
      if (my $model = $self->models->{$table}) {
        if (!defined $default_schema || $default_schema eq $schema) {
          push @{$model->columns}, $column;
        }
      }
      if (my $fullqualified_model = $self->models->{"$schema.$table"}) {
        push @{$fullqualified_model->columns}, $column;
      }
    }
  );
  return $self;
}

sub show_datatype {
  my ($self, $table) = @_;
  croak "Table name must be specified" unless defined $table;
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
  croak "Table name must be specified" unless defined $t;
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
        croak qq{type name of $into section must be lower case}
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
            croak qq{Filter "$fname" is not registered" } . _subname
              unless exists $self->filters->{$fname};
            
            $filter = $self->filters->{$fname};
          }
          
          my $schema = $column_info->{TABLE_SCHEM};
          my $default_schema = $self->default_schema;
          if (!defined $default_schema || $default_schema eq $schema) {
            $self->{"_$into"}{key}{$table}{$column} = $filter;
            $self->{"_$into"}{dot}{"$table.$column"} = $filter;
          }
          
          $self->{"_$into"}{key}{"$schema.$table"}{$column} = $filter;
          $self->{"_$into"}{dot}{"$schema.$table.$column"} = $filter;
        }
      });
    }

    # From
    for my $i (1 .. 2) {
      $type_rule->{"from$i"} = _array_to_hash($type_rule->{"from$i"});
      for my $data_type (keys %{$type_rule->{"from$i"} || {}}) {
        croak qq{data type of from$i section must be lower case or number}
          if $data_type =~ /[A-Z]/;
        my $fname = $type_rule->{"from$i"}{$data_type};
        if (defined $fname && ref $fname ne 'CODE') {
          croak qq{Filter "$fname" is not registered" } . _subname
            unless exists $self->filters->{$fname};
          
          $type_rule->{"from$i"}{$data_type} = $self->filters->{$fname};
        }
      }
    }
    
    return $self;
  }
  
  return $self->{type_rule} || {};
}

sub update {
  my $self = shift;

  # Options
  my $param = @_ % 2 ? shift : undef;
  my %opt = @_;
  $param ||= {};
  
  # Don't allow update all rows
  croak qq{update method where option must be specified } . _subname
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
  my $w = $self->_where_clause_and_param($opt{where}, delete $opt{id}, $opt{primary_key}, $opt{table});
  
  # Merge update parameter with where parameter
  $param = $self->merge_param($param, $w->{param});
  
  # Update statement
  my $sql = "update ";
  $sql .= "$opt{prefix} " if defined $opt{prefix};
  $sql .= $self->_tq($opt{table}) . " set $assign_clause $w->{clause} ";
  
  # Execute query
  $opt{statement} = 'update';
  $self->execute($sql, $param, %opt);
}

sub update_all { shift->update(@_, allow_update_all => 1) };

sub update_or_insert {

  _deprecate('0.39', "DBIx::Custom::update_or_insert method is DEPRECATED!");

  my ($self, $param, %opt) = @_;
  croak "update_or_insert method need primary_key and id option "
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
  else { croak "selected row must be one " . _subname }
}

sub values_clause {
  my ($self, $param, $opts) = @_;
  
  my $wrap = $opts->{wrap} || {};
  
  # Create insert parameter tag
  my ($q, $p) = $self->_qp;
  
  # values clause(performance is important)
  '(' .
  join(
    ', ',
    map { "$q$_$p" } sort keys %$param
  ) .
  ') values (' .
  join(
    ', ',
    map {
      ref $param->{$_} eq 'SCALAR' ? ${$param->{$_}} :
      $wrap->{$_} ? $wrap->{$_}->(":$_") :
      ":$_";
    } sort keys %$param
  ) .
  ')'
}

sub _multi_values_clause {
  my ($self, $params, $opts) = @_;
  
  my $wrap = $opts->{wrap} || {};
  
  # Create insert parameter tag
  my ($q, $p) = $self->_qp;
  
  # Multi values clause
  my $clause = '(' . join(', ', map { "$q$_$p" } sort keys %{$params->[0]}) . ') values ';
  
  for (1 .. @$params) {
    $clause .= '(' . join(', ', 
      map {
        ref $params->[0]->{$_} eq 'SCALAR' ? ${$params->[0]->{$_}} :
        $wrap->{$_} ? $wrap->{$_}->(":$_") :
        ":$_";
      } sort keys %{$params->[0]}
    ) . '), '
  }
  $clause =~ s/, $//;
  return $clause;
}

sub where { DBIx::Custom::Where->new(dbi => shift, @_) }

sub _create_query {
  
  my ($self, $source, $after_build_sql, $prepare_attr) = @_;
  
  $prepare_attr ||= {};
  
  # Create query
  my $sql = " " . $source || '';
  my @columns;
  my $c = $self->{safety_character};
  my $re = $c eq 'a-zA-Z0-9_'
    ? qr/(.*?[^\\]):([$c\.]+)(?:\{(.*?)\})?(.*)/so
    : qr/(.*?[^\\]):([$c\.]+)(?:\{(.*?)\})?(.*)/s;
  my %duplicate;
  my $duplicate;
  # Parameter regex
  $sql =~ s/([0-9]):/$1\\:/g;
  my $new_sql = '';
  while ($sql =~ /$re/) {
    push @columns, $2;
    $duplicate = 1 if ++$duplicate{$columns[-1]} > 1;
    ($new_sql, $sql) = defined $3 ?
      ($new_sql . "$1$2 $3 ?", " $4") : ($new_sql . "$1?", " $4");
  }
  $new_sql .= $sql;
  $new_sql =~ s/\\:/:/g if index($new_sql, "\\:") != -1;
  
  # Create query
  my $query = {sql => $new_sql, columns => \@columns, duplicate => $duplicate};

  # Filter SQL
  $query->{sql} = $after_build_sql->($query->{sql}) if $after_build_sql;
  
  # Save sql
  $self->{last_sql} = $query->{sql};
  
  # Prepare statement handle
  my $sth;
  eval { $sth = $self->dbh->prepare($query->{sql}, $prepare_attr) };
  
  if ($@) {
    $self->_croak($@, qq{. Following SQL is executed.\n}
                    . qq{$query->{sql}\n} . _subname);
  }
  
  # Set statement handle
  $query->{sth} = $sth;
  
  # Set filters
  $query->{filters} = $self->{filters} || $self->filters;
  
  return $query;
}

sub _create_bind_values {
  my ($self, $params, $columns, $filter, $type_filters, $bind_type) = @_;
  
  $bind_type = _array_to_hash($bind_type) if ref $bind_type eq 'ARRAY';
  
  # Create bind values
  my @bind;
  my @types;
  my %count;
  my %not_exists;
  for my $column (@$columns) {
    
    # Bind value
    if(ref $params->{$column} eq 'ARRAY') {
      my $i = $count{$column} || 0;
      $i += $not_exists{$column} || 0;
      my $found;
      for (my $k = $i; $i < @{$params->{$column}}; $k++) {
        if (ref $params->{$column}->[$k] eq 'DBIx::Custom::NotExists') {
            $not_exists{$column}++;
        }
        else  {
          push @bind, $params->{$column}->[$k];
          $found = 1;
          last
        }
      }
      next unless $found;
    }
    else { push @bind, $params->{$column} }
    
    # Filter
    if (my $f = $filter->{$column} || '') {
      $bind[-1] = $f->($bind[-1]);
    }
    
    # Type rule
    if ($self->{_type_rule_is_called}) {
      my $tf1 = $self->{"_into1"}->{dot}->{$column}
        || $type_filters->{1}->{$column};
      $bind[-1] = $tf1->($bind[-1]) if $tf1;
      my $tf2 = $self->{"_into2"}->{dot}->{$column}
        || $type_filters->{2}->{$column};
      $bind[-1] = $tf2->($bind[-1]) if $tf2;
    }
   
    # Bind types
    push @types, $bind_type->{$column};
    
    # Count up 
    $count{$column}++;
  }
  
  return (\@bind, \@types);
}

sub _id_to_param {
  my ($self, $id, $primary_keys, $table) = @_;
  
  # Check primary key
  croak "primary_key option " .
        "must be specified when id option is used" . _subname
    unless defined $primary_keys;
  $primary_keys = [$primary_keys] unless ref $primary_keys eq 'ARRAY';
  
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
  croak qq{"dsn" must be specified } . _subname
    unless $dsn;
  my $user        = $self->user;
  my $password    = $self->password;
  my $option = $self->option;
  $option = {%{$self->default_option}, %$option};
  
  # Connect
  my $dbh;
  eval { $dbh = DBI->connect($dsn, $user, $password, $option) };
  
  # Connect error
  croak "$@ " . _subname if $@;
  
  return $dbh;
}

sub _croak {
  my ($self, $error, $append) = @_;
  
  # Append
  $append ||= "";
  
  # Verbose
  if ($Carp::Verbose) { croak $error }
  
  # Not verbose
  else {
    # Remove line and module information
    my $at_pos = rindex($error, ' at ');
    $error = substr($error, 0, $at_pos);
    $error =~ s/\s+$//;
    croak "$error$append";
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
    croak qq{join clause must have two table name after "on" keyword. } .
        qq{"$join_clause" is passed }  . _subname
      unless defined $table1 && defined $table2;
    croak qq{right side table of "$join_clause" must be unique } . _subname
      if exists $tree->{$table2};
    croak qq{Same table "$table1" is specified} . _subname
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
  my ($self, $where, $id, $primary_key, $table) = @_;

  $where ||= {};
  $where = $self->_id_to_param($id, $primary_key, $table) if defined $id;
  my $w = {};

  if (ref $where eq 'HASH') {
    my $clause = [];
    my $column_join = '';
    for my $column (keys %$where) {
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
      $obj = $self->where(clause => $where->[0], param => $where->[1]);
    }
    
    # Check where argument
    croak qq{"where" must be hash reference or DBIx::Custom::Where object}
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

1;

=head1 NAME

DBIx::Custom - DBI extension to execute insert, update, delete, and select easily

=head1 SYNOPSIS

  use DBIx::Custom;
  
  # Connect
  my $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=dbname",
    user => 'ken',
    password => '!LFKD%$&',
    option => {mysql_enable_utf8 => 1}
  );

  # Insert 
  $dbi->insert({title => 'Perl', author => 'Ken'}, table  => 'book');
  
  # Update 
  $dbi->update({title => 'Perl', author => 'Ken'}, table  => 'book',
    where  => {id => 5});
  
  # Delete
  $dbi->delete(table  => 'book', where => {author => 'Ken'});

  # Select
  #   select title, author from book where author = ?
  my $result = $dbi->select(
    ['title', 'author'],
    table  => 'book',
    where  => {author => 'Ken'}
  );

  # Select, more complex
  #   select book.title as book.title,
  #     book.author as book.author,
  #     comnapy.name as company.name
  #   form book
  #     left outer join company on book.company_id = company.id
  #   where book.author = ?
  #   order by id limit 0, 5
  my $result = $dbi->select(
    [
      {book => [qw/title author/]},
      {company => ['name']}
    ],
    table  => 'book',
    where  => {'book.author' => 'Ken'},
    join => ['left outer join company on book.company_id = company.id'],
    append => 'order by id limit 0, 5'
  );
  
  # Get all rows or only one row
  my $rows = $result->all;
  my $row = $result->one;
  
  # Execute SQL.
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

=head2 async_conf EXPERIMENTAL

  my $async_conf = $dbi->async_conf;
  $dbi = $dbi->async_conf($conf);

Setting when C<async> option is used.

  # MySQL
  $dbi->async_conf({
    prepare_attr => {async => 1},
    fh => sub { shift->dbh->mysql_fd }
    my $dbi = shift;
  });

C<prepare_attr> is DBI's C<prepare> method second argument,
C<fh> is callback that return file handle to watch.

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

=head2 default_schema EXPERIMETNAL

  my $default_schema = $self->default_schema;
  $dbi = $self->default_schema('public');

schema name. if database has multiple schema,
type_rule->{into} filter don't work well.

If you set C<default_schema>, type_rule->{into} filter work well.

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
C<each_column>, C<each_table>, C<type_rule>,
and C<setup_model> methods ignore matching tables.

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

=head2 async EXPERIMENTAL (Currently, Only work in MySQL)

  async => sub {
    my ($dbi, $result) = @_;
    ...
  };

Database async access. L<AnyEvent> is required.

This is C<mysql> async access example.

  use AnyEvent;

  my $cond = AnyEvent->condvar;

  my $timer = AnyEvent->timer(
    interval => 1,
    cb => sub { 1 }
  );

  my $count = 0;

  $dbi->execute('SELECT SLEEP(1), 3', undef,
    prepare_attr => {async => 1}, statement => 'select',
    async => sub {
      my ($dbi, $result) = @_;
      my $row = $result->fetch_one;
      is($row->[1], 3, 'before');
      $cond->send if ++$count == 2;
    }
  );

  $dbi->select('key1', table => 'table1', prepare_attr => {async => 1},
    async => sub {
      my ($dbi, $result) = @_;
      my $row = $result->fetch_one;
      is($row->[0], 1, 'after1');
      $dbi->select('key1', table => 'table1', prepare_attr => {async => 1},
        async => sub {
          my ($dbi, $result) = @_;
          my $row = $result->fetch_one;
          is($row->[0], 1, 'after2');
          $cond->send if ++$count == 2;
        }
      )
    }
  );

  $cond->recv;

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

=head2 count

  my $count = $dbi->count(table => 'book');

Get rows count.

Options is same as C<select> method's ones.

=head2 create_model

  my $model = $dbi->create_model(
    table => 'book',
    primary_key => 'id',
    join => [
      'inner join company on book.comparny_id = company.id'
    ],
  );

Create L<DBIx::Custom::Model> object and initialize model.
the module is also used from C<model> method.

 $dbi->model('book')->select(...);

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

=item C<id>

  id => 4
  id => [4, 5]

ID corresponding to C<primary_key>.
You can delete rows by C<id> and C<primary_key>.

  $dbi->delete(
    primary_key => ['id1', 'id2'],
    id => [4, 5],
    table => 'book',
  );

The above is same as the following one.

  $dbi->delete(where => {id1 => 4, id2 => 5}, table => 'book');

=item C<prefix>

  prefix => 'some'

prefix before table name section.

  delete some from book

=item C<table>

  table => 'book'

Table name.

=item C<where>

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

=item C<after_build_sql> 

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

=item C<append>

  append => 'order by name'

Append some statement after SQL.

=item C<prepare_attr> EXPERIMENTAL

  prepare_attr => {async => 1}

Statemend handle attributes,
this is L<DBI>'s C<prepare> method second argument.

=item C<bind_type>

Specify database bind data type.

  bind_type => [image => DBI::SQL_BLOB]
  bind_type => [[qw/image audio/] => DBI::SQL_BLOB]

This is used to bind parameter by C<bind_param> of statement handle.

  $sth->bind_param($pos, $value, DBI::SQL_BLOB);

=item C<filter>
  
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

=item C<reuse>
  
  reuse => $hash_ref

Reuse query object if the hash reference variable is set.
  
  my $queries = {};
  $dbi->execute($sql, $param, reuse => $queries);

This will improved performance when you want to execute same query repeatedly
because generally creating query object is slow.

=item C<primary_key>

  primary_key => 'id'
  primary_key => ['id1', 'id2']

Priamry key. This is used for C<id> option.

=item C<select> EXPERIMETAL

  select => 1

If you set C<select> to 1, this statement become select statement
and return value is always L<DBIx::Custom::Result> object.

=item C<table>
  
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

=item C<table_alias>

  table_alias => {worker => 'user'} # {ALIAS => TABLE}

Table alias. Key is alias table name, value is real table name, .
If you set C<table_alias>, you can enable C<into1> and C<into2> type rule
on alias table name.

=item C<type_rule_off>

  type_rule_off => 1

Turn C<into1> and C<into2> type rule off.

=item C<type_rule1_off>

  type_rule1_off => 1

Turn C<into1> type rule off.

=item C<type_rule2_off>

  type_rule2_off => 1

Turn C<into2> type rule off.

=back

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

=item C<bulk_insert>

  bulk_insert => 1

bulk insert is executed if database support bulk insert and 
multiple parameters is passed to C<insert>.
The SQL like the following one is executed.

  insert into book (id, title) values (?, ?), (?, ?);

=item C<ctime>

  ctime => 'created_time'

Created time column name. time when row is created is set to the column.
default time format is "YYYY-mm-dd HH:MM:SS", which can be changed by
C<now> attribute.

=item C<id>

  id => 4
  id => [4, 5]

ID corresponding to C<primary_key>.
You can insert a row by C<id> and C<primary_key>.

  $dbi->insert(
    {title => 'Perl', author => 'Ken'}
    primary_key => ['id1', 'id2'],
    id => [4, 5],
    table => 'book'
  );

The above is same as the following one.

  $dbi->insert(
    {id1 => 4, id2 => 5, title => 'Perl', author => 'Ken'},
    table => 'book'
  );

=item C<prefix>

  prefix => 'or replace'

prefix before table name section

  insert or replace into book

=item C<table>

  table => 'book'

Table name.

=item C<mtime>

This option is same as C<update> method C<mtime> option.

=item C<wrap>

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

=item C<column>
  
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

=item C<id>

  id => 4
  id => [4, 5]

ID corresponding to C<primary_key>.
You can select rows by C<id> and C<primary_key>.

  $dbi->select(
    primary_key => ['id1', 'id2'],
    id => [4, 5],
    table => 'book'
  );

The above is same as the following one.

  $dbi->select(
    where => {id1 => 4, id2 => 5},
    table => 'book'
  );
  
=item C<param>

  param => {'table2.key3' => 5}

Parameter shown before where clause.
  
For example, if you want to contain named placeholder in join clause, 
you can pass parameter by C<param> option.

  join  => ['inner join (select * from table2 where table2.key3 = :table2.key3)' . 
            ' as table2 on table1.key1 = table2.key1']

=item C<prefix>

  prefix => 'SQL_CALC_FOUND_ROWS'

Prefix of column clause

  select SQL_CALC_FOUND_ROWS title, author from book;

=item C<join>

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

=item C<table>

  table => 'book'

Table name.

=item C<where>
  
  # (1) Hash reference
  where => {author => 'Ken', 'title' => ['Perl', 'Ruby']}
  # -> where author = 'Ken' and title in ('Perl', 'Ruby')
  
  # (2) DBIx::Custom::Where object
  where => $dbi->where(
    clause => ['and', ':author{=}', ':title{like}'],
    param  => {author => 'Ken', title => '%Perl%'}
  )
  # -> where author = 'Ken' and title like '%Perl%'
  
  # (3) Array reference[Array refenrece, Hash reference]
  where => [
    ['and', ':author{=}', ':title{like}'],
    {author => 'Ken', title => '%Perl%'}
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

=head2 setup_model

  $dbi->setup_model;

Setup all model objects.
C<columns> of model object is automatically set, parsing database information.

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

=item C<id>

  id => 4
  id => [4, 5]

ID corresponding to C<primary_key>.
You can update rows by C<id> and C<primary_key>.

  $dbi->update(
    {title => 'Perl', author => 'Ken'}
    primary_key => ['id1', 'id2'],
    id => [4, 5],
    table => 'book'
  );

The above is same as the following one.

  $dbi->update(
    {title => 'Perl', author => 'Ken'}
    where => {id1 => 4, id2 => 5},
    table => 'book'
  );

=item C<prefix>

  prefix => 'or replace'

prefix before table name section

  update or replace book

=item C<table>

  table => 'book'

Table name.

=item C<where>

Same as C<select> method's C<where> option.

=item C<wrap>

  wrap => {price => sub { "max($_[0])" }}

placeholder wrapped string.

If the following statement

  $dbi->update({price => 100}, table => 'book',
    {price => sub { "$_[0] + 5" }});

is executed, the following SQL is executed.

  update book set price =  ? + 5;

=item C<mtime>

  mtime => 'modified_time'

Modified time column name. time row is updated is set to the column.
default time format is C<YYYY-mm-dd HH:MM:SS>, which can be changed by
C<now> attribute.

=back

=head2 update_all

  $dbi->update_all({title => 'Perl'}, table => 'book', );

Execute update statement for all rows.
Options is same as C<update> method.

=head2 update_or_insert

  # ID
  $dbi->update_or_insert(
    {title => 'Perl'},
    table => 'book',
    id => 1,
    primary_key => 'id',
    option => {
      select => {
         append => 'for update'
      }
    }
  );

Update or insert.

C<update_or_insert> method execute C<select> method first to find row.
If the row is exists, C<update> is executed.
If not, C<insert> is executed.

C<OPTIONS>

C<update_or_insert> method use all common option
in C<select>, C<update>, C<delete>, and has the following new ones.

=over 4

=item C<option>

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

=item C<select_option>

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
  DBIx::Custom::helper method # will be removed at 2022/5/1
  DBIx::Custom AUTOLOAD feature # will be removed at 2022/5/1
  DEPRECATE DBIx::Custom::Model AUTOLOAD feature # will be removed at 2022/5/1

L<DBIx::Custom::Result>
  
  # Options
  kv method's multi option (from 0.28) # will be removed at 2018/3/1

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

Copyright 2009-2017 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
