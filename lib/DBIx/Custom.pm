package DBIx::Custom;
use Object::Simple -base;

our $VERSION = '0.1717';
use 5.008001;

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder;
use DBIx::Custom::Where;
use DBIx::Custom::Model;
use DBIx::Custom::Tag;
use DBIx::Custom::Order;
use DBIx::Custom::Util qw/_array_to_hash _subname/;
use Encode qw/encode encode_utf8 decode_utf8/;
use Scalar::Util qw/weaken/;

use constant DEBUG => $ENV{DBIX_CUSTOM_DEBUG} || 0;
use constant DEBUG_ENCODING => $ENV{DBIX_CUSTOM_DEBUG_ENCODING} || 'UTF-8';

has [qw/connector dsn password quote user exclude_table user_table_info
        user_column_info/],
    cache => 0,
    cache_method => sub {
        sub {
            my $self = shift;
            
            $self->{_cached} ||= {};
            
            if (@_ > 1) {
                $self->{_cached}{$_[0]} = $_[1];
            }
            else {
                return $self->{_cached}{$_[0]};
            }
        }
    },
    dbi_option => sub { {} },
    default_dbi_option => sub {
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1
        }
    },
    filters => sub {
        {
            encode_utf8 => sub { encode_utf8($_[0]) },
            decode_utf8 => sub { decode_utf8($_[0]) }
        }
    },
    last_sql => '',
    models => sub { {} },
    query_builder => sub {
        my $self = shift;
        my $builder = DBIx::Custom::QueryBuilder->new(dbi => $self);
        weaken $builder->{dbi};
        return $builder;
    },
    result_class  => 'DBIx::Custom::Result',
    safety_character => '\w',
    separator => '.',
    stash => sub { {} },
    tag_parse => 1;

sub available_datatype {
    my $self = shift;
    
    my $data_types = '';
    foreach my $i (-1000 .. 1000) {
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

sub assign_param {
    my ($self, $param) = @_;
    
    # Create set tag
    my @params;
    my $safety = $self->safety_character;
    foreach my $column (sort keys %$param) {
        croak qq{"$column" is not safety column name } . _subname
          unless $column =~ /^[$safety\.]+$/;
        my $column_quote = $self->_q($column);
        $column_quote =~ s/\./$self->_q(".")/e;
        push @params, ref $param->{$column} eq 'SCALAR'
          ? "$column_quote = " . ${$param->{$column}}
          : "$column_quote = :$column";

    }
    my $tag = join(', ', @params);
    
    return $tag;
}

sub column {
    my $self = shift;
    my $option = pop if ref $_[-1] eq 'HASH';
    my $real_table = shift;
    my $columns = shift;
    my $table = $option->{alias} || $real_table;
    
    # Columns
    unless ($columns) {
        $columns ||= $self->model($real_table)->columns;
    }
    
    # Separator
    my $separator = $self->separator;
    
    # Column clause
    my @column;
    $columns ||= [];
    push @column, $self->_q($table) . "." . $self->_q($_) .
      " as " . $self->_q("${table}${separator}$_")
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
        my $dbi_option = {%{$self->dbi_options}, %{$self->dbi_option}};
        my $connector = DBIx::Connector->new($dsn, $user, $password,
          {%{$self->default_dbi_option} , %$dbi_option});
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
            croak "connector must have dbh() method " . _subname
              unless ref $connector && $connector->can('dbh');
              
            $self->{dbh} = $connector->dbh;
        }
        
        # Connect
        $self->{dbh} ||= $self->_connect;
        
        # Quote
        if (!defined $self->reserved_word_quote && !defined $self->quote) {
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
    my ($self, %args) = @_;

    # Arguments
    my $table = $args{table} || '';
    croak qq{"table" option must be specified. } . _subname
      unless $table;
    my $where            = delete $args{where} || {};
    my $append           = delete $args{append};
    my $allow_delete_all = delete $args{allow_delete_all};
    my $where_param      = delete $args{where_param} || {};
    my $id = delete $args{id};
    my $primary_key = delete $args{primary_key};
    croak "update method primary_key option " .
          "must be specified when id is specified " . _subname
      if defined $id && !defined $primary_key;
    $primary_key = [$primary_key] unless ref $primary_key eq 'ARRAY';
    my $prefix = delete $args{prefix};
    
    # Where
    $where = $self->_create_param_from_id($id, $primary_key) if defined $id;
    my $where_clause = '';
    if (ref $where eq 'ARRAY' && !ref $where->[0]) {
        $where_clause = "where " . $where->[0];
        $where_param = $where->[1];
    }
    elsif (ref $where) {
        $where = $self->_where_to_obj($where);
        $where_param = keys %$where_param
                     ? $self->merge_param($where_param, $where->param)
                     : $where->param;
        
        # String where
        $where_clause = $where->to_string;
    }
    elsif ($where) { $where_clause = "where $where" }
    croak qq{"where" must be specified } . _subname
      if $where_clause eq '' && !$allow_delete_all;

    # Delete statement
    my @sql;
    push @sql, "delete";
    push @sql, $prefix if defined $prefix;
    push @sql, "from " . $self->_q($table) . " $where_clause";
    push @sql, $append if defined $append;
    my $sql = join(' ', @sql);
    
    # Execute query
    return $self->execute($sql, $where_param, table => $table, %args);
}

sub delete_all { shift->delete(allow_delete_all => 1, @_) }

sub DESTROY {}

sub create_model {
    my $self = shift;
    
    # Arguments
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $args->{dbi} = $self;
    my $model_class = delete $args->{model_class} || 'DBIx::Custom::Model';
    my $model_name  = delete $args->{name};
    my $model_table = delete $args->{table};
    $model_name ||= $model_table;
    
    # Create model
    my $model = $model_class->new($args);
    weaken $model->{dbi};
    $model->name($model_name) unless $model->name;
    $model->table($model_table) unless $model->table;
    
    # Apply filter(DEPRECATED logic)
    if ($model->{filter}) {
        my $filter = ref $model->filter eq 'HASH'
                   ? [%{$model->filter}]
                   : $model->filter;
        $filter ||= [];
        warn "DBIx::Custom::Model filter method is DEPRECATED!"
          if @$filter;
        $self->_apply_filter($model->table, @$filter);
    }
    
    # Set model
    $self->model($model->name, $model);
    
    return $self->model($model->name);
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
        my %tables;
        $self->each_table(sub { $tables{$_[1]}++ });

        # Iterate all tables
        my @tables = sort keys %tables;
        for (my $i = 0; $i < @tables; $i++) {
            my $table = $tables[$i];
            
            # Iterate all columns
            my $sth_columns;
            eval {$sth_columns = $self->dbh->column_info(undef, undef, $table, '%')};
            next if $@;
            while (my $column_info = $sth_columns->fetchrow_hashref) {
                my $column = $column_info->{COLUMN_NAME};
                $self->$cb($table, $column, $column_info);
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

our %VALID_ARGS = map { $_ => 1 } qw/append allow_delete_all
  allow_update_all bind_type column filter id join param prefix primary_key
  query relation sqlfilter table table_alias type type_rule_off type_rule1_off
  type_rule2_off wrap/;

sub execute {
    my $self = shift;
    my $query = shift;
    my $param;
    $param = shift if @_ % 2;
    my %args = @_;
    
    # Arguments
    my $p = delete $args{param} || {};
    $param ||= $p;
    my $tables = delete $args{table} || [];
    $tables = [$tables] unless ref $tables eq 'ARRAY';
    my $filter = delete $args{filter};
    $filter = _array_to_hash($filter);
    my $bind_type = delete $args{bind_type} || delete $args{type};
    $bind_type = _array_to_hash($bind_type);
    my $type_rule_off = delete $args{type_rule_off};
    my $type_rule_off_parts = {
        1 => delete $args{type_rule1_off},
        2 => delete $args{type_rule2_off}
    };
    my $query_return = delete $args{query};
    my $table_alias = delete $args{table_alias} || {};
    my $sqlfilter = $args{sqlfilter};
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $VALID_ARGS{$name};
    }
    
    $query = $self->_create_query($query, $sqlfilter) unless ref $query;
    
    # Save query
    $self->last_sql($query->sql);

    return $query if $query_return;
    
    # DEPRECATED! Merge query filter
    $filter ||= $query->{filter} || {};
    
    # Tables
    unshift @$tables, @{$query->{tables} || []};
    my $main_table = @{$tables}[-1];
    
    # DEPRECATED! Cleanup tables
    $tables = $self->_remove_duplicate_table($tables, $main_table)
      if @$tables > 1;
    
    # Type rule
    my $type_filters = {};
    unless ($type_rule_off) {
        foreach my $i (1, 2) {
            unless ($type_rule_off_parts->{$i}) {
                $type_filters->{$i} = {};
                foreach my $alias (keys %$table_alias) {
                    my $table = $table_alias->{$alias};
                    
                    foreach my $column (keys %{$self->{"_into$i"}{key}{$table} || {}}) {
                        $type_filters->{$i}->{"$alias.$column"} = $self->{"_into$i"}{key}{$table}{$column};
                    }
                }
                $type_filters->{$i} = {%{$type_filters->{$i}}, %{$self->{"_into$i"}{key}{$main_table} || {}}}
                  if $main_table;
            }
        }
    }
    
    # DEPRECATED! Applied filter
    if ($self->{filter}{on}) {
        my $applied_filter = {};
        foreach my $table (@$tables) {
            $applied_filter = {
                %$applied_filter,
                %{$self->{filter}{out}->{$table} || {}}
            }
        }
        $filter = {%$applied_filter, %$filter};
    }
    
    # Replace filter name to code
    foreach my $column (keys %$filter) {
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
    
    # Create bind values
    my $bind = $self->_create_bind_values(
        $param,
        $query->columns,
        $filter,
        $type_filters,
        $bind_type
    );

    # Execute
    my $sth = $query->sth;
    my $affected;
    eval {
        for (my $i = 0; $i < @$bind; $i++) {
            my $bind_type = $bind->[$i]->{bind_type};
            $sth->bind_param(
                $i + 1,
                $bind->[$i]->{value},
                $bind_type ? $bind_type : ()
            );
        }
        $affected = $sth->execute;
    };
    
    $self->_croak($@, qq{. Following SQL is executed.\n}
      . qq{$query->{sql}\n} . _subname) if $@;
    
    # DEBUG message
    if (DEBUG) {
        print STDERR "SQL:\n" . $query->sql . "\n";
        my @output;
        foreach my $b (@$bind) {
            my $value = $b->{value};
            $value = 'undef' unless defined $value;
            $value = encode(DEBUG_ENCODING(), $value)
              if utf8::is_utf8($value);
            push @output, $value;
        }
        print STDERR "Bind values: " . join(', ', @output) . "\n\n";
    }
    
    # Select statement
    if ($sth->{NUM_OF_FIELDS}) {
        
        # DEPRECATED! Filter
        my $filter = {};
        if ($self->{filter}{on}) {
            $filter->{in}  = {};
            $filter->{end} = {};
            push @$tables, $main_table if $main_table;
            foreach my $table (@$tables) {
                foreach my $way (qw/in end/) {
                    $filter->{$way} = {
                        %{$filter->{$way}},
                        %{$self->{filter}{$way}{$table} || {}}
                    };
                }
            }
        }
        
        # Result
        my $result = $self->result_class->new(
            sth => $sth,
            dbi => $self,
            default_filter => $self->{default_in_filter},
            filter => $filter->{in} || {},
            end_filter => $filter->{end} || {},
            type_rule => {
                from1 => $self->type_rule->{from1},
                from2 => $self->type_rule->{from2}
            },
        );

        return $result;
    }
    
    # Not select statement
    else { return $affected }
}

sub get_table_info {
    my ($self, %args) = @_;
    
    my $exclude = delete $args{exclude};
    croak qq/"$_" is wrong option/ for keys %args;
    
    my $table_info = [];
    $self->each_table(
        sub { push @$table_info, {table => $_[1], info => $_[2] } },
        exclude => $exclude
    );
    
    return [sort {$a->{table} cmp $b->{table} } @$table_info];
}

sub get_column_info {
    my ($self, %args) = @_;
    
    my $exclude_table = delete $args{exclude_table};
    croak qq/"$_" is wrong option/ for keys %args;
    
    my $column_info = [];
    $self->each_column(
        sub { push @$column_info, {table => $_[1], column => $_[2], info => $_[3] } },
        exclude_table => $exclude_table
    );
    
    return [
      sort {$a->{table} cmp $b->{table} || $a->{column} cmp $b->{column} }
        @$column_info];
}

sub insert {
    my $self = shift;
    
    # Arguments
    my $param;
    $param = shift if @_ % 2;
    my %args = @_;
    my $table  = delete $args{table};
    croak qq{"table" option must be specified } . _subname
      unless defined $table;
    my $p = delete $args{param} || {};
    $param  ||= $p;
    my $append = delete $args{append} || '';
    my $id = delete $args{id};
    my $primary_key = delete $args{primary_key};
    croak "insert method primary_key option " .
          "must be specified when id is specified " . _subname
      if defined $id && !defined $primary_key;
    $primary_key = [$primary_key] unless ref $primary_key eq 'ARRAY';
    my $prefix = delete $args{prefix};

    # Merge parameter
    if (defined $id) {
        my $id_param = $self->_create_param_from_id($id, $primary_key);
        $param = $self->merge_param($id_param, $param);
    }

    # Insert statement
    my @sql;
    push @sql, "insert";
    push @sql, $prefix if defined $prefix;
    push @sql, "into " . $self->_q($table) . " " . $self->insert_param($param);
    push @sql, $append if defined $append;
    my $sql = join (' ', @sql);
    
    # Execute query
    return $self->execute($sql, $param, table => $table, %args);
}

sub insert_param {
    my ($self, $param) = @_;
    
    # Create insert parameter tag
    my $safety = $self->safety_character;
    my @columns;
    my @placeholders;
    foreach my $column (sort keys %$param) {
        croak qq{"$column" is not safety column name } . _subname
          unless $column =~ /^[$safety\.]+$/;
        my $column_quote = $self->_q($column);
        $column_quote =~ s/\./$self->_q(".")/e;
        push @columns, $column_quote;
        push @placeholders, ref $param->{$column} eq 'SCALAR'
          ? ${$param->{$column}} : ":$column";
    }
    
    return '(' . join(', ', @columns) . ') ' . 'values ' .
           '(' . join(', ', @placeholders) . ')'
}

sub include_model {
    my ($self, $name_space, $model_infos) = @_;
    
    # Name space
    $name_space ||= '';
    
    # Get Model infomations
    unless ($model_infos) {

        # Load name space module
        croak qq{"$name_space" is invalid class name } . _subname
          if $name_space =~ /[^\w:]/;
        eval "use $name_space";
        croak qq{Name space module "$name_space.pm" is needed. $@ }
            . _subname
          if $@;
        
        # Search model modules
        my $path = $INC{"$name_space.pm"};
        $path =~ s/\.pm$//;
        opendir my $dh, $path
          or croak qq{Can't open directory "$path": $! } . _subname
        $model_infos = [];
        while (my $module = readdir $dh) {
            push @$model_infos, $module
              if $module =~ s/\.pm$//;
        }
        close $dh;
    }
    
    # Include models
    foreach my $model_info (@$model_infos) {
        
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
        else { $model_class = $model_name = $model_table = $model_info }
        my $mclass = "${name_space}::$model_class";
        croak qq{"$mclass" is invalid class name } . _subname
          if $mclass =~ /[^\w:]/;
        unless ($mclass->can('isa')) {
            eval "use $mclass";
            croak "$@ " . _subname if $@;
        }
        
        # Create model
        my $args = {};
        $args->{model_class} = $mclass if $mclass;
        $args->{name}        = $model_name if $model_name;
        $args->{table}       = $model_table if $model_table;
        $self->create_model($args);
    }
    
    return $self;
}

sub map_param {
    my $self = shift;
    my $param = shift;
    my %map = @_;
    
    # Mapping
    my $map_param = {};
    foreach my $key (keys %map) {
        my $value_cb;
        my $condition;
        my $map_key;
        
        # Get mapping information
        if (ref $map{$key} eq 'ARRAY') {
            foreach my $some (@{$map{$key}}) {
                $map_key = $some unless ref $some;
                $condition = $some->{if} if ref $some eq 'HASH';
                $value_cb = $some if ref $some eq 'CODE';
            }
        }
        else {
            $map_key = $map{$key};
        }
        $value_cb ||= sub { $_[0] };
        $condition ||= sub { defined $_[0] && length $_[0] };

        # Map parameter
        my $value;
        if (ref $condition eq 'CODE') {
            $map_param->{$map_key} = $value_cb->($param->{$key})
              if $condition->($param->{$key});
        }
        elsif ($condition eq 'exists') {
            $map_param->{$map_key} = $value_cb->($param->{$key})
              if exists $param->{$key};
        }
        else { croak qq/Condition must be code reference or "exists" / . _subname }
    }
    
    return $map_param;
}

sub merge_param {
    my ($self, @params) = @_;
    
    # Merge parameters
    my $merge = {};
    foreach my $param (@params) {
        foreach my $column (keys %$param) {
            my $param_is_array = ref $param->{$column} eq 'ARRAY' ? 1 : 0;
            
            if (exists $merge->{$column}) {
                $merge->{$column} = [$merge->{$column}]
                  unless ref $merge->{$column} eq 'ARRAY';
                push @{$merge->{$column}},
                  ref $param->{$column} ? @{$param->{$column}} : $param->{$column};
            }
            else {
                $merge->{$column} = $param->{$column};
            }
        }
    }
    
    return $merge;
}

sub method {
    my $self = shift;
    
    # Register method
    my $methods = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_methods} = {%{$self->{_methods} || {}}, %$methods};
    
    return $self;
}

sub model {
    my ($self, $name, $model) = @_;
    
    # Set model
    if ($model) {
        $self->models->{$name} = $model;
        return $self;
    }
    
    # Check model existance
    croak qq{Model "$name" is not included } . _subname
      unless $self->models->{$name};
    
    # Get model
    return $self->models->{$name};
}

sub mycolumn {
    my ($self, $table, $columns) = @_;
    
    # Create column clause
    my @column;
    $columns ||= [];
    push @column, $self->_q($table) . "." . $self->_q($_) .
      " as " . $self->_q($_)
      for @$columns;
    
    return join (', ', @column);
}

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attributes
    my @attrs = keys %$self;
    foreach my $attr (@attrs) {
        croak qq{Invalid attribute: "$attr"} . _subname
          unless $self->can($attr);
    }

    # DEPRECATED
    $self->{_tags} = {
        '?'     => \&DBIx::Custom::Tag::placeholder,
        '='     => \&DBIx::Custom::Tag::equal,
        '<>'    => \&DBIx::Custom::Tag::not_equal,
        '>'     => \&DBIx::Custom::Tag::greater_than,
        '<'     => \&DBIx::Custom::Tag::lower_than,
        '>='    => \&DBIx::Custom::Tag::greater_than_equal,
        '<='    => \&DBIx::Custom::Tag::lower_than_equal,
        'like'  => \&DBIx::Custom::Tag::like,
        'in'    => \&DBIx::Custom::Tag::in,
        'insert_param' => \&DBIx::Custom::Tag::insert_param,
        'update_param' => \&DBIx::Custom::Tag::update_param
    };
    
    return $self;
}

my $not_exists = bless {}, 'DBIx::Custom::NotExists';
sub not_exists { $not_exists }

sub order {
    my $self = shift;
    return DBIx::Custom::Order->new(dbi => $self, @_);
}

sub register_filter {
    my $self = shift;
    
    # Register filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->filters({%{$self->filters}, %$filters});
    
    return $self;
}

sub select {
    my ($self, %args) = @_;

    # Arguments
    my $table = delete $args{table};
    my $tables = ref $table eq 'ARRAY' ? $table
               : defined $table ? [$table]
               : [];
    my $columns   = delete $args{column};
    my $where     = delete $args{where} || {};
    my $append    = delete $args{append};
    my $join      = delete $args{join} || [];
    {
      my $ref = ref $join;
      if (not $ref) {
	$join = [ $join ];
      } else {
	croak qq{"join" must be array reference } . _subname
	  unless $ref eq 'ARRAY';
      }
    }
    my $relation = delete $args{relation};
    warn "select() relation option is DEPRECATED!"
      if $relation;
    my $param = delete $args{param} || {}; # DEPRECATED!
    warn "select() param option is DEPRECATED!"
      if keys %$param;
    my $where_param = delete $args{where_param} || $param || {};
    my $wrap = delete $args{wrap};
    my $id = delete $args{id};
    my $primary_key = delete $args{primary_key};
    croak "update method primary_key option " .
          "must be specified when id is specified " . _subname
      if defined $id && !defined $primary_key;
    $primary_key = [$primary_key] unless ref $primary_key eq 'ARRAY';
    my $prefix = delete $args{prefix};
    
    # Add relation tables(DEPRECATED!);
    $self->_add_relation_table($tables, $relation);
    
    # Select statement
    my @sql;
    push @sql, 'select';
    
    # Prefix
    push @sql, $prefix if defined $prefix;
    
    # Column clause
    if ($columns) {
        $columns = [$columns] unless ref $columns eq 'ARRAY';
        foreach my $column (@$columns) {
            if (ref $column eq 'HASH') {
                $column = $self->column(%$column) if ref $column eq 'HASH';
            }
            elsif (ref $column eq 'ARRAY') {
                if (@$column == 3 && $column->[1] eq 'as') {
                    warn "[COLUMN, as => ALIAS] is DEPRECATED! use [COLUMN => ALIAS]";
                    splice @$column, 1, 1;
                }
                
                $column = join(' ', $column->[0], 'as', $self->_q($column->[1]));
            }
            unshift @$tables, @{$self->_search_tables($column)};
            push @sql, ($column, ',');
        }
        pop @sql if $sql[-1] eq ',';
    }
    else { push @sql, '*' }
    
    # Table
    push @sql, 'from';
    if ($relation) {
        my $found = {};
        foreach my $table (@$tables) {
            push @sql, ($self->_q($table), ',') unless $found->{$table};
            $found->{$table} = 1;
        }
    }
    else {
        my $main_table = $tables->[-1] || '';
        push @sql, $self->_q($main_table);
    }
    pop @sql if ($sql[-1] || '') eq ',';
    croak "Not found table name " . _subname
      unless $tables->[-1];

    # Add tables in parameter
    unshift @$tables,
            @{$self->_search_tables(join(' ', keys %$where_param) || '')};
    
    # Where
    my $where_clause = '';
    $where = $self->_create_param_from_id($id, $primary_key) if defined $id;
    if (ref $where eq 'ARRAY' && !ref $where->[0]) {
        $where_clause = "where " . $where->[0];
        $where_param = $where->[1];
    }
    elsif (ref $where) {
        $where = $self->_where_to_obj($where);
        $where_param = keys %$where_param
                     ? $self->merge_param($where_param, $where->param)
                     : $where->param;
        
        # String where
        $where_clause = $where->to_string;
    }
    elsif ($where) { $where_clause = "where $where" }
    
    # Add table names in where clause
    unshift @$tables, @{$self->_search_tables($where_clause)};
    
    # Push join
    $self->_push_join(\@sql, $join, $tables);
    
    # Add where clause
    push @sql, $where_clause;
    
    # Relation(DEPRECATED!);
    $self->_push_relation(\@sql, $tables, $relation, $where_clause eq '' ? 1 : 0);
    
    # Append
    push @sql, $append if defined $append;
    
    # Wrap
    if ($wrap) {
        croak "wrap option must be array refrence " . _subname
          unless ref $wrap eq 'ARRAY';
        unshift @sql, $wrap->[0];
        push @sql, $wrap->[1];
    }
    
    # SQL
    my $sql = join (' ', @sql);
    
    # Execute query
    my $result = $self->execute($sql, $where_param, table => $tables, %args);
    
    return $result;
}

sub setup_model {
    my $self = shift;
    
    # Setup model
    $self->each_column(
        sub {
            my ($self, $table, $column, $column_info) = @_;
            if (my $model = $self->models->{$table}) {
                push @{$model->columns}, $column;
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
        my $data_type = $data_types->[$i];
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
        my $typename = $infos->{TYPE_NAME};
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
    
    if (@_) {
        my $type_rule = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        # Into
        foreach my $i (1 .. 2) {
            my $into = "into$i";
            my $exists_into = exists $type_rule->{$into};
            $type_rule->{$into} = _array_to_hash($type_rule->{$into});
            $self->{type_rule} = $type_rule;
            $self->{"_$into"} = {};
            foreach my $type_name (keys %{$type_rule->{$into} || {}}) {
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
                    if  (defined $filter && ref $filter ne 'CODE') 
                    {
                        my $fname = $filter;
                        croak qq{Filter "$fname" is not registered" } . _subname
                          unless exists $self->filters->{$fname};
                        
                        $filter = $self->filters->{$fname};
                    }

                    $self->{"_$into"}{key}{$table}{$column} = $filter;
                    $self->{"_$into"}{dot}{"$table.$column"} = $filter;
                }
            });
        }

        # From
        foreach my $i (1 .. 2) {
            $type_rule->{"from$i"} = _array_to_hash($type_rule->{"from$i"});
            foreach my $data_type (keys %{$type_rule->{"from$i"} || {}}) {
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

    # Arguments
    my $param;
    $param = shift if @_ % 2;
    my %args = @_;
    my $table = delete $args{table} || '';
    croak qq{"table" option must be specified } . _subname
      unless $table;
    my $p = delete $args{param} || {};
    $param  ||= $p;
    my $where = delete $args{where} || {};
    my $where_param = delete $args{where_param} || {};
    my $append = delete $args{append} || '';
    my $allow_update_all = delete $args{allow_update_all};
    my $id = delete $args{id};
    my $primary_key = delete $args{primary_key};
    croak "update method primary_key option " .
          "must be specified when id is specified " . _subname
      if defined $id && !defined $primary_key;
    $primary_key = [$primary_key] unless ref $primary_key eq 'ARRAY';
    my $prefix = delete $args{prefix};

    # Update clause
    my $update_clause = $self->update_param($param);

    # Where
    $where = $self->_create_param_from_id($id, $primary_key) if defined $id;
    my $where_clause = '';
    if (ref $where eq 'ARRAY' && !ref $where->[0]) {
        $where_clause = "where " . $where->[0];
        $where_param = $where->[1];
    }
    elsif (ref $where) {
        $where = $self->_where_to_obj($where);
        $where_param = keys %$where_param
                     ? $self->merge_param($where_param, $where->param)
                     : $where->param;
        
        # String where
        $where_clause = $where->to_string;
    }
    elsif ($where) { $where_clause = "where $where" }
    croak qq{"where" must be specified } . _subname
      if "$where_clause" eq '' && !$allow_update_all;
    
    # Merge param
    $param = $self->merge_param($param, $where_param) if keys %$where_param;
    
    # Update statement
    my @sql;
    push @sql, "update";
    push @sql, $prefix if defined $prefix;
    push @sql, $self->_q($table) . " $update_clause $where_clause";
    push @sql, $append if defined $append;
    
    # SQL
    my $sql = join(' ', @sql);
    
    # Execute query
    return $self->execute($sql, $param, table => $table, %args);
}

sub update_all { shift->update(allow_update_all => 1, @_) };

sub update_param {
    my ($self, $param, $opt) = @_;
    
    # Create update parameter tag
    my $tag = $self->assign_param($param);
    $tag = "set $tag" unless $opt->{no_set};

    return $tag;
}

sub where { DBIx::Custom::Where->new(dbi => shift, @_) }

sub _create_query {
    
    my ($self, $source, $sqlfilter) = @_;
    
    # Cache
    my $cache = $self->cache;
    
    # Query
    my $query;
    
    # Get cached query
    if ($cache) {
        
        # Get query
        my $q = $self->cache_method->($self, $source);
        
        # Create query
        if ($q) {
            $query = DBIx::Custom::Query->new($q);
            $query->{filters} = $self->filters;
        }
    }
    
    # Create query
    unless ($query) {

        # Create query
        my $builder = $self->query_builder;
        $query = $builder->build_query($source);

        # Remove reserved word quote
        if (my $q = $self->_quote) {
            $q = quotemeta($q);
            $_ =~ s/[$q]//g for @{$query->columns}
        }

        # Save query to cache
        $self->cache_method->(
            $self, $source,
            {
                sql     => $query->sql, 
                columns => $query->columns,
                tables  => $query->{tables} || []
            }
        ) if $cache;
    }

    # Filter SQL
    if ($sqlfilter) {
        my $sql = $query->sql;
        $sql = $sqlfilter->($sql);
        $query->sql($sql);
    }
        
    # Save sql
    $self->last_sql($query->sql);
    
    # Prepare statement handle
    my $sth;
    eval { $sth = $self->dbh->prepare($query->{sql})};
    
    if ($@) {
        $self->_croak($@, qq{. Following SQL is executed.\n}
                        . qq{$query->{sql}\n} . _subname);
    }
    
    # Set statement handle
    $query->sth($sth);
    
    # Set filters
    $query->{filters} = $self->filters;
    
    return $query;
}

sub _create_bind_values {
    my ($self, $params, $columns, $filter, $type_filters, $bind_type) = @_;
    
    # Create bind values
    my $bind = [];
    my $count = {};
    my $not_exists = {};
    foreach my $column (@$columns) {
        
        # Value
        my $value;
        if(ref $params->{$column} eq 'ARRAY') {
            my $i = $count->{$column} || 0;
            $i += $not_exists->{$column} || 0;
            my $found;
            for (my $k = $i; $i < @{$params->{$column}}; $k++) {
                if (ref $params->{$column}->[$k] eq 'DBIx::Custom::NotExists') {
                    $not_exists->{$column}++;
                }
                else  {
                    $value = $params->{$column}->[$k];
                    $found = 1;
                    last
                }
            }
            next unless $found;
        }
        else { $value = $params->{$column} }
        
        # Filter
        my $f = $filter->{$column} || $self->{default_out_filter} || '';
        $value = $f->($value) if $f;
        
        # Type rule
        foreach my $i (1 .. 2) {
            my $type_filter = $type_filters->{$i};
            my $tf = $self->{"_into$i"}->{dot}->{$column} || $type_filter->{$column};
            $value = $tf->($value) if $tf;
        }
        
        # Bind values
        push @$bind, {value => $value, bind_type => $bind_type->{$column}};
        
        # Count up 
        $count->{$column}++;
    }
    
    return $bind;
}

sub _create_param_from_id {
    my ($self, $id, $primary_keys) = @_;
    
    # Create parameter
    my $param = {};
    if (defined $id) {
        $id = [$id] unless ref $id;
        croak qq{"id" must be constant value or array reference}
            . " (" . (caller 1)[3] . ")"
          unless !ref $id || ref $id eq 'ARRAY';
        croak qq{"id" must contain values same count as primary key}
            . " (" . (caller 1)[3] . ")"
          unless @$primary_keys eq @$id;
        for(my $i = 0; $i < @$primary_keys; $i ++) {
           $param->{$primary_keys->[$i]} = $id->[$i];
        }
    }
    
    return $param;
}

sub _connect {
    my $self = shift;
    
    # Attributes
    my $dsn = $self->data_source;
    warn "data_source is DEPRECATED!\n"
      if $dsn;
    $dsn ||= $self->dsn;
    croak qq{"dsn" must be specified } . _subname
      unless $dsn;
    my $user        = $self->user;
    my $password    = $self->password;
    my $dbi_option = {%{$self->dbi_options}, %{$self->dbi_option}};
    warn "dbi_options is DEPRECATED! use dbi_option instead\n"
      if keys %{$self->dbi_options};
    
    $dbi_option = {%{$self->default_dbi_option}, %$dbi_option};
    
    # Connect
    my $dbh;
    eval {
        $dbh = DBI->connect(
            $dsn,
            $user,
            $password,
            $dbi_option
        );
    };
    
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
        
        # Remove line and module infromation
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
    foreach my $table (@$tables) {
        if ($tree->{$table}) {
            $need_tables->{$table} = 1;
            $self->_need_tables($tree, $need_tables, [$tree->{$table}{parent}])
        }
    }
}

sub _push_join {
    my ($self, $sql, $join, $join_tables) = @_;
    
    # No join
    return unless @$join;
    
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
            my $c = $self->safety_character;
            my $join_re = qr/(?:^|\s)($c+)\.$c+\s+=\s+($c+)\.$c+/;
            if ($j_clause =~ $join_re) {
                $table1 = $1;
                $table2 = $2;
            }
        }
        croak qq{join clause must have two table name after "on" keyword. } .
              qq{"$join_clause" is passed }  . _subname
          unless defined $table1 && defined $table2;
        croak qq{right side table of "$join_clause" must be unique }
            . _subname
          if exists $tree->{$table2};
        croak qq{Same table "$table1" is specified} . _subname
          if $table1 eq $table2;
        $tree->{$table2}
          = {position => $i, parent => $table1, join => $join_clause};
    }
    
    # Search need tables
    my $need_tables = {};
    $self->_need_tables($tree, $need_tables, $join_tables);
    my @need_tables = sort { $tree->{$a}{position} <=> $tree->{$b}{position} } keys %$need_tables;
    
    # Add join clause
    foreach my $need_table (@need_tables) {
        push @$sql, $tree->{$need_table}{join};
    }
}

sub _quote {
    my $self = shift;
    
    return defined $self->reserved_word_quote ? $self->reserved_word_quote
         : defined $self->quote ? $self->quote
         : '';
}

sub _q {
    my ($self, $value, $quotemeta) = @_;
    
    my $quote = $self->_quote;
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
    
    return "$q$value$p";
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
    my $safety_character = $self->safety_character;
    my $q = $self->_quote;
    my $quoted_safety_character_re = $self->_q("?([$safety_character]+)", 1);
    my $table_re = $q ? qr/(?:^|[^$safety_character])${quoted_safety_character_re}?\./
                      : qr/(?:^|[^$safety_character])([$safety_character]+)\./;
    while ($source =~ /$table_re/g) {
        push @$tables, $1;
    }
    
    return $tables;
}

sub _where_to_obj {
    my ($self, $where) = @_;
    
    my $obj;
    
    # Hash
    if (ref $where eq 'HASH') {
        my $clause = ['and'];
        my $q = $self->_quote;
        foreach my $column (keys %$where) {
            my $table;
            my $c;
            if ($column =~ /(?:(.*?)\.)?(.*)/) {
                $table = $1;
                $c = $2;
            }
            
            my $table_quote;
            $table_quote = $self->_q($table) if defined $table;
            my $column_quote = $self->_q($c);
            $column_quote = $table_quote . '.' . $column_quote
              if defined $table_quote;
            push @$clause, "$column_quote = :$column" for keys %$where;
        }
        $obj = $self->where(clause => $clause, param => $where);
    }
    
    # DBIx::Custom::Where object
    elsif (ref $where eq 'DBIx::Custom::Where') {
        $obj = $where;
    }
    
    # Array
    elsif (ref $where eq 'ARRAY') {
        $obj = $self->where(
            clause => $where->[0],
            param  => $where->[1]
        );
    }
    
    # Check where argument
    croak qq{"where" must be hash reference or DBIx::Custom::Where object}
        . qq{or array reference, which contains where clause and parameter}
        . _subname
      unless ref $obj eq 'DBIx::Custom::Where';
    
    return $obj;
}

sub _apply_filter {
    my ($self, $table, @cinfos) = @_;

    # Initialize filters
    $self->{filter} ||= {};
    $self->{filter}{on} = 1;
    $self->{filter}{out} ||= {};
    $self->{filter}{in} ||= {};
    $self->{filter}{end} ||= {};
    
    # Usage
    my $usage = "Usage: \$dbi->apply_filter(" .
                "TABLE, COLUMN1, {in => INFILTER1, out => OUTFILTER1, end => ENDFILTER1}, " .
                "COLUMN2, {in => INFILTER2, out => OUTFILTER2, end => ENDFILTER2}, ...)";
    
    # Apply filter
    for (my $i = 0; $i < @cinfos; $i += 2) {
        
        # Column
        my $column = $cinfos[$i];
        if (ref $column eq 'ARRAY') {
            foreach my $c (@$column) {
                push @cinfos, $c, $cinfos[$i + 1];
            }
            next;
        }
        
        # Filter infomation
        my $finfo = $cinfos[$i + 1] || {};
        croak "$usage (table: $table) " . _subname
          unless  ref $finfo eq 'HASH';
        foreach my $ftype (keys %$finfo) {
            croak "$usage (table: $table) " . _subname
              unless $ftype eq 'in' || $ftype eq 'out' || $ftype eq 'end'; 
        }
        
        # Set filters
        foreach my $way (qw/in out end/) {
        
            # Filter
            my $filter = $finfo->{$way};
            
            # Filter state
            my $state = !exists $finfo->{$way} ? 'not_exists'
                      : !defined $filter        ? 'not_defined'
                      : ref $filter eq 'CODE'   ? 'code'
                      : 'name';
            
            # Filter is not exists
            next if $state eq 'not_exists';
            
            # Check filter name
            croak qq{Filter "$filter" is not registered } . _subname
              if  $state eq 'name'
               && ! exists $self->filters->{$filter};
            
            # Set filter
            my $f = $state eq 'not_defined' ? undef
                  : $state eq 'code'        ? $filter
                  : $self->filters->{$filter};
            $self->{filter}{$way}{$table}{$column} = $f;
            $self->{filter}{$way}{$table}{"$table.$column"} = $f;
            $self->{filter}{$way}{$table}{"${table}__$column"} = $f;
            $self->{filter}{$way}{$table}{"${table}-$column"} = $f;
        }
    }
    
    return $self;
}

# DEPRECATED!
sub create_query {
    warn "create_query is DEPRECATED! use query option of each method";
    shift->_create_query(@_);
}

# DEPRECATED!
sub apply_filter {
    my $self = shift;
    
    warn "apply_filter is DEPRECATED!";
    return $self->_apply_filter(@_);
}

# DEPRECATED!
our %SELECT_AT_ARGS = (%VALID_ARGS, where => 1, primary_key => 1);
sub select_at {
    my ($self, %args) = @_;

    warn "select_at is DEPRECATED! use update and id option instead";

    # Arguments
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    my $where = delete $args{where};
    my $param = delete $args{param};
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $SELECT_AT_ARGS{$name};
    }
    
    # Table
    croak qq{"table" option must be specified } . _subname
      unless $args{table};
    my $table = ref $args{table} ? $args{table}->[-1] : $args{table};
    
    # Create where parameter
    my $where_param = $self->_create_param_from_id($where, $primary_keys);
    
    return $self->select(where => $where_param, %args);
}

# DEPRECATED!
our %DELETE_AT_ARGS = (%VALID_ARGS, where => 1, primary_key => 1);
sub delete_at {
    my ($self, %args) = @_;

    warn "delete_at is DEPRECATED! use update and id option instead";
    
    # Arguments
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    my $where = delete $args{where};
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $DELETE_AT_ARGS{$name};
    }
    
    # Create where parameter
    my $where_param = $self->_create_param_from_id($where, $primary_keys);
    
    return $self->delete(where => $where_param, %args);
}

# DEPRECATED!
our %UPDATE_AT_ARGS = (%VALID_ARGS, where => 1, primary_key => 1);
sub update_at {
    my $self = shift;

    warn "update_at is DEPRECATED! use update and id option instead";
    
    # Arguments
    my $param;
    $param = shift if @_ % 2;
    my %args = @_;
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    my $where = delete $args{where};
    my $p = delete $args{param} || {};
    $param  ||= $p;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $UPDATE_AT_ARGS{$name};
    }
    
    # Create where parameter
    my $where_param = $self->_create_param_from_id($where, $primary_keys);
    
    return $self->update(where => $where_param, param => $param, %args);
}

# DEPRECATED!
our %INSERT_AT_ARGS = (%VALID_ARGS, where => 1, primary_key => 1);
sub insert_at {
    my $self = shift;
    
    warn "insert_at is DEPRECATED! use insert and id option instead";
    
    # Arguments
    my $param;
    $param = shift if @_ % 2;
    my %args = @_;
    my $primary_key = delete $args{primary_key};
    $primary_key = [$primary_key] unless ref $primary_key;
    my $where = delete $args{where};
    my $p = delete $args{param} || {};
    $param  ||= $p;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $INSERT_AT_ARGS{$name};
    }
    
    # Create where parameter
    my $where_param = $self->_create_param_from_id($where, $primary_key);
    $param = $self->merge_param($where_param, $param);
    
    return $self->insert(param => $param, %args);
}

# DEPRECATED!
sub register_tag {
    my $self = shift;
    
    warn "register_tag is DEPRECATED!";
    
    # Merge tag
    my $tags = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_tags} = {%{$self->{_tags} || {}}, %$tags};
    
    return $self;
}

# DEPRECATED!
sub register_tag_processor {
    my $self = shift;
    warn "register_tag_processor is DEPRECATED!";
    # Merge tag
    my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_tags} = {%{$self->{_tags} || {}}, %{$tag_processors}};
    return $self;
}

# DEPRECATED!
has 'data_source';
has dbi_options => sub { {} };
has filter_check  => 1;
has 'reserved_word_quote';

# DEPRECATED!
sub default_bind_filter {
    my $self = shift;
    
    warn "default_bind_filter is DEPRECATED!";
    
    if (@_) {
        my $fname = $_[0];
        
        if (@_ && !$fname) {
            $self->{default_out_filter} = undef;
        }
        else {
            croak qq{Filter "$fname" is not registered}
              unless exists $self->filters->{$fname};
        
            $self->{default_out_filter} = $self->filters->{$fname};
        }
        return $self;
    }
    
    return $self->{default_out_filter};
}

# DEPRECATED!
sub default_fetch_filter {
    my $self = shift;

    warn "default_fetch_filter is DEPRECATED!";
    
    if (@_) {
        my $fname = $_[0];

        if (@_ && !$fname) {
            $self->{default_in_filter} = undef;
        }
        else {
            croak qq{Filter "$fname" is not registered}
              unless exists $self->filters->{$fname};
        
            $self->{default_in_filter} = $self->filters->{$fname};
        }
        
        return $self;
    }
    
    return $self->{default_in_filter};
}

# DEPRECATED!
sub insert_param_tag {
    warn "insert_param_tag is DEPRECATED! " .
         "use insert_param instead!";
    return shift->insert_param(@_);
}

# DEPRECATED!
sub update_param_tag {
    warn "update_param_tag is DEPRECATED! " .
         "use update_param instead";
    return shift->update_param(@_);
}
# DEPRECATED!
sub _push_relation {
    my ($self, $sql, $tables, $relation, $need_where) = @_;
    
    if (keys %{$relation || {}}) {
        push @$sql, $need_where ? 'where' : 'and';
        foreach my $rcolumn (keys %$relation) {
            my $table1 = (split (/\./, $rcolumn))[0];
            my $table2 = (split (/\./, $relation->{$rcolumn}))[0];
            push @$tables, ($table1, $table2);
            push @$sql, ("$rcolumn = " . $relation->{$rcolumn},  'and');
        }
    }
    pop @$sql if $sql->[-1] eq 'and';    
}

# DEPRECATED!
sub _add_relation_table {
    my ($self, $tables, $relation) = @_;
    
    if (keys %{$relation || {}}) {
        foreach my $rcolumn (keys %$relation) {
            my $table1 = (split (/\./, $rcolumn))[0];
            my $table2 = (split (/\./, $relation->{$rcolumn}))[0];
            my $table1_exists;
            my $table2_exists;
            foreach my $table (@$tables) {
                $table1_exists = 1 if $table eq $table1;
                $table2_exists = 1 if $table eq $table2;
            }
            unshift @$tables, $table1 unless $table1_exists;
            unshift @$tables, $table2 unless $table2_exists;
        }
    }
}

1;

=head1 NAME

DBIx::Custom - Execute insert, update, delete, and select statement easily

=head1 SYNOPSIS

    use DBIx::Custom;
    
    # Connect
    my $dbi = DBIx::Custom->connect(
        dsn => "dbi:mysql:database=dbname",
        user => 'ken',
        password => '!LFKD%$&',
        dbi_option => {mysql_enable_utf8 => 1}
    );

    # Insert 
    $dbi->insert({title => 'Perl', author => 'Ken'}, table  => 'book');
    
    # Update 
    $dbi->update({title => 'Perl', author => 'Ken'}, table  => 'book',
      where  => {id => 5});
    
    # Delete
    $dbi->delete(table  => 'book', where => {author => 'Ken'});

    # Select
    my $result = $dbi->select(table  => 'book',
      column => ['title', 'author'], where  => {author => 'Ken'});

    # Select, more complex
    my $result = $dbi->select(
        table  => 'book',
        column => [
            {book => [qw/title author/]},
            {company => ['name']}
        ],
        where  => {'book.author' => 'Ken'},
        join => ['left outer join company on book.company_id = company.id'],
        append => 'order by id limit 5'
    );
    
    # Fetch
    while (my $row = $result->fetch) {
        
    }
    
    # Fetch as hash
    while (my $row = $result->fetch_hash) {
        
    }
    
    # Execute SQL with parameter.
    $dbi->execute(
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

Filtering by data type or column name(EXPERIMENTAL)

=item *

Create C<order by> clause flexibly(EXPERIMENTAL)

=back

=head1 DOCUMENTATION

L<DBIx::Custom::Guide> - How to use L<DBIx::Custom>

L<DBIx::Custom Wiki|https://github.com/yuki-kimoto/DBIx-Custom/wiki>
- Theare are various examples.

Module documentations - 
L<DBIx::Custom::Result>,
L<DBIx::Custom::Query>,
L<DBIx::Custom::Where>,
L<DBIx::Custom::Model>,
L<DBIx::Custom::Order>

=head1 ATTRIBUTES

=head2 C<connector>

    my $connector = $dbi->connector;
    $dbi = $dbi->connector($connector);

Connection manager object. if C<connector> is set, you can get C<dbh>
through connection manager. Conection manager object must have C<dbh> mehtod.

This is L<DBIx::Connector> example. Please pass
C<default_dbi_option> to L<DBIx::Connector> C<new> method.

    my $connector = DBIx::Connector->new(
        "dbi:mysql:database=$database",
        $user,
        $password,
        DBIx::Custom->new->default_dbi_option
    );
    
    my $dbi = DBIx::Custom->connect(connector => $connector);

If C<connector> is set to 1 when connect method is called,
L<DBIx::Connector> is automatically set to C<connector>

    my $dbi = DBIx::Custom->connect(
      dsn => $dsn, user => $user, password => $password, connector => 1);
    
    my $connector = $dbi->connector; # DBIx::Connector

Note that L<DBIx::Connector> must be installed.

=head2 C<dsn>

    my $dsn = $dbi->dsn;
    $dbi = $dbi->dsn("DBI:mysql:database=dbname");

Data source name, used when C<connect> method is executed.

=head2 C<dbi_option>

    my $dbi_option = $dbi->dbi_option;
    $dbi = $dbi->dbi_option($dbi_option);

L<DBI> option, used when C<connect> method is executed.
Each value in option override the value of C<default_dbi_option>.

=head2 C<default_dbi_option>

    my $default_dbi_option = $dbi->default_dbi_option;
    $dbi = $dbi->default_dbi_option($default_dbi_option);

L<DBI> default option, used when C<connect> method is executed,
default to the following values.

    {
        RaiseError => 1,
        PrintError => 0,
        AutoCommit => 1,
    }

=head2 C<filters>

    my $filters = $dbi->filters;
    $dbi = $dbi->filters(\%filters);

Filters, registered by C<register_filter> method.

=head2 C<last_sql>

    my $last_sql = $dbi->last_sql;
    $dbi = $dbi->last_sql($last_sql);

Get last successed SQL executed by C<execute> method.

=head2 C<models>

    my $models = $dbi->models;
    $dbi = $dbi->models(\%models);

Models, included by C<include_model> method.

=head2 C<password>

    my $password = $dbi->password;
    $dbi = $dbi->password('lkj&le`@s');

Password, used when C<connect> method is executed.

=head2 C<query_builder>

    my $builder = $dbi->query_builder;

Creat query builder. This is L<DBIx::Custom::QueryBuilder>.

=head2 C<quote>

     my quote = $dbi->quote;
     $dbi = $dbi->quote('"');

Reserved word quote.
Default to double quote '"' except for mysql.
In mysql, default to back quote '`'

You can set quote pair.

    $dbi->quote('[]');

=head2 C<result_class>

    my $result_class = $dbi->result_class;
    $dbi = $dbi->result_class('DBIx::Custom::Result');

Result class, default to L<DBIx::Custom::Result>.

=head2 C<safety_character>

    my $safety_character = $self->safety_character;
    $dbi = $self->safety_character($character);

Regex of safety character for table and column name, default to '\w'.
Note that you don't have to specify like '[\w]'.

=head2 C<separator>

    my $separator = $self->separator;
    $dbi = $self->separator($separator);

Separator whichi join table and column.
This is used by C<column> and C<mycolumn> method.

=head2 C<exclude_table EXPERIMENTAL>

    my $exclude_table = $self->exclude_table;
    $dbi = $self->exclude_table(qr/pg_/);

Regex matching system table.
this regex match is used by C<each_table> method and C<each_column> method
System table is ignored.
C<type_rule> method and C<setup_model> method call
C<each_table>, so if you set C<exclude_table> properly,
The performance is up.

=head2 C<tag_parse>

    my $tag_parse = $dbi->tag_parse(0);
    $dbi = $dbi->tag_parse;

Enable DEPRECATED tag parsing functionality, default to 1.
If you want to disable tag parsing functionality, set to 0.

=head2 C<user>

    my $user = $dbi->user;
    $dbi = $dbi->user('Ken');

User name, used when C<connect> method is executed.

=head2 C<user_column_info EXPERIMENTAL>

    my $user_column_info = $dbi->user_column_info;
    $dbi = $dbi->user_column_info($user_column_info);

You can set the following data.

    [
        {table => 'book', column => 'title', info => {...}},
        {table => 'author', column => 'name', info => {...}}
    ]

Usually, you can set return value of C<get_column_info>.

    my $user_column_info
      = $dbi->get_column_info(exclude_table => qr/^system/);
    $dbi->user_column_info($user_column_info);

If C<user_column_info> is set, C<each_column> use C<user_column_info>
to find column info.

=head2 C<user_table_info EXPERIMENTAL>

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

=head2 C<available_datatype> EXPERIMENTAL

    print $dbi->available_datatype;

Get available data types. You can use these data types
in C<type rule>'s C<from1> and C<from2> section.

=head2 C<available_typename> EXPERIMENTAL

    print $dbi->available_typename;

Get available type names. You can use these type names in
C<type_rule>'s C<into1> and C<into2> section.

=head2 C<assign_param> EXPERIMENTAL

    my $assign_param = $dbi->assign_param({title => 'a', age => 2});

Create assign parameter.

    title = :title, author = :author

This is equal to C<update_param> exept that set is not added.

=head2 C<column>

    my $column = $dbi->column(book => ['author', 'title']);

Create column clause. The follwoing column clause is created.

    book.author as "book.author",
    book.title as "book.title"

You can change separator by C<separator> attribute.

    # Separator is double underbar
    $dbi->separator('__');
    
    book.author as "book__author",
    book.title as "book__title"

    # Separator is hyphen
    $dbi->separator('-');
    
    book.author as "book-author",
    book.title as "book-title"
    
=head2 C<connect>

    my $dbi = DBIx::Custom->connect(
        dsn => "dbi:mysql:database=dbname",
        user => 'ken',
        password => '!LFKD%$&',
        dbi_option => {mysql_enable_utf8 => 1}
    );

Connect to the database and create a new L<DBIx::Custom> object.

L<DBIx::Custom> is a wrapper of L<DBI>.
C<AutoCommit> and C<RaiseError> options are true, 
and C<PrintError> option is false by default.

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

=head2 C<dbh>

    my $dbh = $dbi->dbh;

Get L<DBI> database handle. if C<connector> is set, you can get
database handle through C<connector> object.

=head2 C<delete>

    $dbi->delete(table => 'book', where => {title => 'Perl'});

Execute delete statement.

The following opitons are available.

=over 4

=item C<append>

Same as C<select> method's C<append> option.

=item C<filter>

Same as C<execute> method's C<filter> option.

=item C<id>

    id => 4
    id => [4, 5]

ID corresponding to C<primary_key>.
You can delete rows by C<id> and C<primary_key>.

    $dbi->delete(
        parimary_key => ['id1', 'id2'],
        id => [4, 5],
        table => 'book',
    );

The above is same as the followin one.

    $dbi->delete(where => {id1 => 4, id2 => 5}, table => 'book');

=item C<prefix>

    prefix => 'some'

prefix before table name section.

    delete some from book

=item C<query>

Same as C<execute> method's C<query> option.

=item C<sqlfilter EXPERIMENTAL>

Same as C<execute> method's C<sqlfilter> option.

=item C<table>

    table => 'book'

Table name.

=item C<where>

Same as C<select> method's C<where> option.

=item C<primary_key>

See C<id> option.

=item C<bind_type>

Same as C<execute> method's C<bind_type> option.

=item C<type_rule_off> EXPERIMENTAL

Same as C<execute> method's C<type_rule_off> option.

=item C<type_rule1_off> EXPERIMENTAL

    type_rule1_off => 1

Same as C<execute> method's C<type_rule1_off> option.

=item C<type_rule2_off> EXPERIMENTAL

    type_rule2_off => 1

Same as C<execute> method's C<type_rule2_off> option.

=back

=head2 C<delete_all>

    $dbi->delete_all(table => $table);

Execute delete statement for all rows.
Options is same as C<delete>.

=head2 C<each_column>

    $dbi->each_column(
        sub {
            my ($dbi, $table, $column, $column_info) = @_;
            
            my $type = $column_info->{TYPE_NAME};
            
            if ($type eq 'DATE') {
                # ...
            }
        }
    );

Iterate all column informations of all table from database.
Argument is callback when one column is found.
Callback receive four arguments, dbi object, table name,
column name and column information.

=head2 C<each_table>

    $dbi->each_table(
        sub {
            my ($dbi, $table, $table_info) = @_;
            
            my $table_name = $table_info->{TABLE_NAME};
        }
    );

Iterate all table informationsfrom database.
Argument is callback when one table is found.
Callback receive three arguments, dbi object, table name,
table information.

=head2 C<execute>

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
Second argunet is data, embedded into column parameter.
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

Note that colons in time format such as 12:13:15 is exeption,
it is not parsed as named placeholder.
If you want to use colon generally, you must escape it by C<\\>

    select * from where title = "aa\\:bb";

The following opitons are available.

=over 4

=item C<bind_type>

Specify database bind data type.

    bind_type => [image => DBI::SQL_BLOB]
    bind_type => [[qw/image audio/] => DBI::SQL_BLOB]

This is used to bind parameter by C<bind_param> of statment handle.

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
registered by by C<register_filter>.
This filter is executed before data is saved into database.
and before type rule filter is executed.

=item C<query>

    query => 1

C<execute> method return L<DBIx::Custom::Query> object, not executing SQL.
You can check SQL or get statment handle.

    my $sql = $query->sql;
    my $sth = $query->sth;
    my $columns = $query->columns;
    
If you want to execute SQL fast, you can do the following way.

    my $query;
    foreach my $row (@$rows) {
      $query ||= $dbi->insert($row, table => 'table1', query => 1);
      $dbi->execute($query, $row, filter => {ab => sub { $_[0] * 2 }});
    }

Statement handle is reused and SQL parsing is finished,
so you can get more performance than normal way.

If you want to execute SQL as possible as fast and don't need filtering.
You can do the following way.
    
    my $query;
    my $sth;
    foreach my $row (@$rows) {
      $query ||= $dbi->insert($row, table => 'book', query => 1);
      $sth ||= $query->sth;
      $sth->execute(map { $row->{$_} } sort keys %$row);
    }

Note that $row must be simple hash reference, such as
{title => 'Perl', author => 'Ken'}.
and don't forget to sort $row values by $row key asc order.

=item C<sqlfilter EXPERIMENTAL> 

SQL filter function.

    sqlfilter => $code_ref

This option is generally for Oracle and SQL Server paging process.
    
    my $limit = sub {
        my ($sql, $count, $offset) = @_;
        
        my $min = $offset + 1;
        my $max = $offset + $count;
        
        $sql = "select * from ( $sql ) as t where rnum >= $min rnum <= $max";
        
        return $sql;
    }
    $dbi->select(... column => ['ROWNUM rnom'], sqlfilter => sub {
        my $sql = shift;
        return $limit->($sql, 100, 50);
    })

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

=item C<table_alias> EXPERIMENTAL

    table_alias => {user => 'hiker'}

Table alias. Key is real table name, value is alias table name.
If you set C<table_alias>, you can enable C<into1> and C<into2> type rule
on alias table name.

=item C<type_rule_off> EXPERIMENTAL

    type_rule_off => 1

Turn C<into1> and C<into2> type rule off.

=item C<type_rule1_off> EXPERIMENTAL

    type_rule1_off => 1

Turn C<into1> type rule off.

=item C<type_rule2_off> EXPERIMENTAL

    type_rule2_off => 1

Turn C<into2> type rule off.

=back

=head2 C<get_column_info EXPERIMENTAL>

    my $tables = $self->get_column_info(exclude_table => qr/^system_/);

get column infomation except for one which match C<exclude_table> pattern.

    [
        {table => 'book', column => 'title', info => {...}},
        {table => 'author', column => 'name' info => {...}}
    ]

=head2 C<get_table_info EXPERIMENTAL>

    my $tables = $self->get_table_info(exclude => qr/^system_/);

get table infomation except for one which match C<exclude> pattern.

    [
        {table => 'book', info => {...}},
        {table => 'author', info => {...}}
    ]

You can set this value to C<user_table_info>.

=head2 C<insert>

    $dbi->insert({title => 'Perl', author => 'Ken'}, table  => 'book');

Execute insert statement. First argument is row data. Return value is
affected row count.

If you want to set constant value to row data, use scalar reference
as parameter value.

    {date => \"NOW()"}

The following opitons are available.

=over 4

=item C<append>

Same as C<select> method's C<append> option.

=item C<bind_type>

Same as C<execute> method's C<bind_type> option.

=item C<filter>

Same as C<execute> method's C<filter> option.

=item C<id>

    id => 4
    id => [4, 5]

ID corresponding to C<primary_key>.
You can insert a row by C<id> and C<primary_key>.

    $dbi->insert(
        {title => 'Perl', author => 'Ken'}
        parimary_key => ['id1', 'id2'],
        id => [4, 5],
        table => 'book'
    );

The above is same as the followin one.

    $dbi->insert(
        {id1 => 4, id2 => 5, title => 'Perl', author => 'Ken'},
        table => 'book'
    );

=item C<prefix>

    prefix => 'or replace'

prefix before table name section

    insert or replace into book

=item C<primary_key>

    primary_key => 'id'
    primary_key => ['id1', 'id2']

Primary key. This is used by C<id> option.

=item C<query>

Same as C<execute> method's C<query> option.

=item C<sqlfilter EXPERIMENTAL>

Same as C<execute> method's C<sqlfilter> option.

=item C<table>

    table => 'book'

Table name.

=item C<type_rule_off> EXPERIMENTAL

Same as C<execute> method's C<type_rule_off> option.

=item C<type_rule1_off> EXPERIMENTAL

    type_rule1_off => 1

Same as C<execute> method's C<type_rule1_off> option.

=item C<type_rule2_off> EXPERIMENTAL

    type_rule2_off => 1

Same as C<execute> method's C<type_rule2_off> option.

=back

=over 4

=head2 C<insert_param>

    my $insert_param = $dbi->insert_param({title => 'a', age => 2});

Create insert parameters.

    (title, author) values (title = :title, age = :age);

=head2 C<include_model>

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

See L<DBIx::Custom::Model> to know model features.

=head2 C<map_param> EXPERIMENTAL

    my $map_param = $dbi->map_param(
        {id => 1, authro => 'Ken', price => 1900},
        'id' => 'book.id',
        'author' => ['book.author' => sub { '%' . $_[0] . '%' }],
        'price' => [
            'book.price', {if => sub { length $_[0] }}
        ]
    );

Map paramters to other key and value. First argument is original
parameter. this is hash reference. Rest argument is mapping.
By default, Mapping is done if the value length is not zero.

=over 4

=item Key mapping

    'id' => 'book.id'

This is only key mapping. Value is same as original one.

    (id => 1) is mapped to ('book.id' => 1) if value length is not zero.

=item Key and value mapping

    'author' => ['book.author' => sub { '%' . $_[0] . '%' }]

This is key and value mapping. Frist element of array reference
is mapped key name, second element is code reference to map the value.

    (author => 'Ken') is mapped to ('book.author' => '%Ken%')
      if value length is not zero.

=item Condition

    'price' => ['book.price', {if => 'exists'}]
    'price' => ['book.price', sub { '%' . $_[0] . '%' }, {if => 'exists'}]
    'price' => ['book.price', {if => sub { defined shift }}]

If you need condition, you can sepecify it. this is code reference
or 'exists'. By default, condition is the following one.

    sub { defined $_[0] && length $_[0] }

=back

=head2 C<merge_param>

    my $param = $dbi->merge_param({key1 => 1}, {key1 => 1, key2 => 2});

Merge parameters.

    {key1 => [1, 1], key2 => 2}

=head2 C<method>

    $dbi->method(
        update_or_insert => sub {
            my $self = shift;
            
            # Process
        },
        find_or_create   => sub {
            my $self = shift;
            
            # Process
        }
    );

Register method. These method is called directly from L<DBIx::Custom> object.

    $dbi->update_or_insert;
    $dbi->find_or_create;

=head2 C<model>

    my $model = $dbi->model('book');

Get a L<DBIx::Custom::Model> object,

=head2 C<mycolumn>

    my $column = $self->mycolumn(book => ['author', 'title']);

Create column clause for myself. The follwoing column clause is created.

    book.author as author,
    book.title as title

=head2 C<new>

    my $dbi = DBIx::Custom->new(
        dsn => "dbi:mysql:database=dbname",
        user => 'ken',
        password => '!LFKD%$&',
        dbi_option => {mysql_enable_utf8 => 1}
    );

Create a new L<DBIx::Custom> object.

=head2 C<not_exists>

    my $not_exists = $dbi->not_exists;

DBIx::Custom::NotExists object, indicating the column is not exists.
This is used by C<clause> of L<DBIx::Custom::Where> .

=head2 C<order> EXPERIMENTAL

    my $order = $dbi->order;

Create a new L<DBIx::Custom::Order> object.

=head2 C<register_filter>

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

=head2 C<type_rule> EXPERIMENTAL

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
If these contain upper case charactor, you convert it to lower case.

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

=head2 C<select>

    my $result = $dbi->select(
        table  => 'book',
        column => ['author', 'title'],
        where  => {author => 'Ken'},
    );
    
Execute select statement.

The following opitons are available.

=over 4

=item C<append>

    append => 'order by title'

Append statement to last of SQL.

=item C<bind_type>

Same as C<execute> method's C<bind_type> option.
    
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

You can specify array of array reference, first argument is
column name, second argument is alias.

    column => [
        ['date(book.register_datetime)' => 'book.register_date']
    ];

Alias is quoted properly and joined.

    date(book.register_datetime) as "book.register_date"

=item C<filter>

Same as C<execute> method's C<filter> option.

=item C<id>

    id => 4
    id => [4, 5]

ID corresponding to C<primary_key>.
You can select rows by C<id> and C<primary_key>.

    $dbi->select(
        parimary_key => ['id1', 'id2'],
        id => [4, 5],
        table => 'book'
    );

The above is same as the followin one.

    $dbi->select(
        where => {id1 => 4, id2 => 5},
        table => 'book'
    );
    
=item C<param> EXPERIMETNAL

    param => {'table2.key3' => 5}

Parameter shown before where clause.
    
For example, if you want to contain tag in join clause, 
you can pass parameter by C<param> option.

    join  => ['inner join (select * from table2 where table2.key3 = :table2.key3)' . 
              ' as table2 on table1.key1 = table2.key1']

=itme C<prefix>

    prefix => 'SQL_CALC_FOUND_ROWS'

Prefix of column cluase

    select SQL_CALC_FOUND_ROWS title, author from book;

=item C<join>

    join => [
        'left outer join company on book.company_id = company_id',
        'left outer join location on company.location_id = location.id'
    ]
        
Join clause. If column cluase or where clause contain table name like "company.name",
join clausees needed when SQL is created is used automatically.

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
the join clause correctly. This is EXPERIMENTAL.

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

=item C<primary_key>

    primary_key => 'id'
    primary_key => ['id1', 'id2']

Primary key. This is used by C<id> option.

=item C<query>

Same as C<execute> method's C<query> option.

=item C<sqlfilter EXPERIMENTAL>

Same as C<execute> method's C<sqlfilter> option

=item C<table>

    table => 'book'

Table name.

=item C<type_rule_off> EXPERIMENTAL

Same as C<execute> method's C<type_rule_off> option.

=item C<type_rule1_off> EXPERIMENTAL

    type_rule1_off => 1

Same as C<execute> method's C<type_rule1_off> option.

=item C<type_rule2_off> EXPERIMENTAL

    type_rule2_off => 1

Same as C<execute> method's C<type_rule2_off> option.

=item C<where>
    
    # Hash refrence
    where => {author => 'Ken', 'title' => 'Perl'}
    
    # DBIx::Custom::Where object
    where => $dbi->where(
        clause => ['and', 'author = :author', 'title like :title'],
        param  => {author => 'Ken', title => '%Perl%'}
    );
    
    # Array reference 1 (array reference, hash referenc). same as above
    where => [
        ['and', 'author = :author', 'title like :title'],
        {author => 'Ken', title => '%Perl%'}
    ];    
    
    # Array reference 2 (String, hash reference)
    where => [
        'title like :title',
        {title => '%Perl%'}
    ]
    
    # String
    where => 'title is null'

Where clause.
    
=item C<wrap> EXPERIMENTAL

Wrap statement. This is array reference.

    wrap => ['select * from (', ') as t where ROWNUM < 10']

This option is for Oracle and SQL Server paging process.

=back

=head2 C<update>

    $dbi->update({title => 'Perl'}, table  => 'book', where  => {id => 4});

Execute update statement. First argument is update row data.

If you want to set constant value to row data, use scalar reference
as parameter value.

    {date => \"NOW()"}

The following opitons are available.

=over 4

=item C<append>

Same as C<select> method's C<append> option.

=item C<bind_type>

Same as C<execute> method's C<bind_type> option.

=item C<filter>

Same as C<execute> method's C<filter> option.

=item C<id>

    id => 4
    id => [4, 5]

ID corresponding to C<primary_key>.
You can update rows by C<id> and C<primary_key>.

    $dbi->update(
        {title => 'Perl', author => 'Ken'}
        parimary_key => ['id1', 'id2'],
        id => [4, 5],
        table => 'book'
    );

The above is same as the followin one.

    $dbi->update(
        {title => 'Perl', author => 'Ken'}
        where => {id1 => 4, id2 => 5},
        table => 'book'
    );

=item C<prefix>

    prefix => 'or replace'

prefix before table name section

    update or replace book

=item C<primary_key>

    primary_key => 'id'
    primary_key => ['id1', 'id2']

Primary key. This is used by C<id> option.

=item C<query>

Same as C<execute> method's C<query> option.

=item C<sqlfilter EXPERIMENTAL>

Same as C<execute> method's C<sqlfilter> option.

=item C<table>

    table => 'book'

Table name.

=item C<type_rule_off> EXPERIMENTAL

Same as C<execute> method's C<type_rule_off> option.

=item C<type_rule1_off> EXPERIMENTAL

    type_rule1_off => 1

Same as C<execute> method's C<type_rule1_off> option.

=item C<type_rule2_off> EXPERIMENTAL

    type_rule2_off => 1

Same as C<execute> method's C<type_rule2_off> option.

=item C<where>

Same as C<select> method's C<where> option.

=back

=head2 C<update_all>

    $dbi->update_all({title => 'Perl'}, table => 'book', );

Execute update statement for all rows.
Options is same as C<update> method.

=head2 C<update_param>

    my $update_param = $dbi->update_param({title => 'a', age => 2});

Create update parameter tag.

    set title = :title, author = :author

=head2 C<where>

    my $where = $dbi->where(
        clause => ['and', 'title = :title', 'author = :author'],
        param => {title => 'Perl', author => 'Ken'}
    );

Create a new L<DBIx::Custom::Where> object.

=head2 C<setup_model>

    $dbi->setup_model;

Setup all model objects.
C<columns> of model object is automatically set, parsing database information.

=head1 ENVIRONMENTAL VARIABLES

=head2 C<DBIX_CUSTOM_DEBUG>

If environment variable C<DBIX_CUSTOM_DEBUG> is set to true,
executed SQL and bind values are printed to STDERR.

=head2 C<show_datatype EXPERIMENTAL>

    $dbi->show_datatype($table);

Show data type of the columns of specified table.

    book
    title: 5
    issue_date: 91

This data type is used in C<type_rule>'s C<from1> and C<from2>.

=head2 C<show_tables EXPERIMETNAL>

    $dbi->show_tables;

Show tables.

=head2 C<show_typename EXPERIMENTAL>

    $dbi->show_typename($table);

Show type name of the columns of specified table.

    book
    title: varchar
    issue_date: date

This type name is used in C<type_rule>'s C<into1> and C<into2>.

=head2 C<DBIX_CUSTOM_DEBUG_ENCODING>

DEBUG output encoding. Default to UTF-8.

=head1 DEPRECATED FUNCTIONALITY

L<DBIx::Custom>

    # Attribute methods
    data_source # will be removed at 2017/1/1
    dbi_options # will be removed at 2017/1/1
    filter_check # will be removed at 2017/1/1
    reserved_word_quote # will be removed at 2017/1/1
    cache_method # will be removed at 2017/1/1
    
    # Methods
    create_query # will be removed at 2017/1/1
    apply_filter # will be removed at 2017/1/1
    select_at # will be removed at 2017/1/1
    delete_at # will be removed at 2017/1/1
    update_at # will be removed at 2017/1/1
    insert_at # will be removed at 2017/1/1
    register_tag # will be removed at 2017/1/1
    default_bind_filter # will be removed at 2017/1/1
    default_fetch_filter # will be removed at 2017/1/1
    insert_param_tag # will be removed at 2017/1/1
    register_tag # will be removed at 2017/1/1
    register_tag_processor # will be removed at 2017/1/1
    update_param_tag # will be removed at 2017/1/1
    
    # Options
    select method relation option # will be removed at 2017/1/1
    select method param option # will be removed at 2017/1/1
    select method column option [COLUMN, as => ALIAS] format
      # will be removed at 2017/1/1
    
    # Others
    execute("select * from {= title}"); # execute method's
                                        # tag parsing functionality
                                        # will be removed at 2017/1/1
    Query caching # will be removed at 2017/1/1

L<DBIx::Custom::Model>

    # Attribute methods
    filter # will be removed at 2017/1/1
    name # will be removed at 2017/1/1
    type # will be removed at 2017/1/1

L<DBIx::Custom::Query>
    
    # Attribute methods
    default_filter # will be removed at 2017/1/1
    table # will be removed at 2017/1/1
    filters # will be removed at 2017/1/1
    
    # Methods
    filter # will be removed at 2017/1/1

L<DBIx::Custom::QueryBuilder>
    
    # Attribute methods
    tags # will be removed at 2017/1/1
    tag_processors # will be removed at 2017/1/1
    
    # Methods
    register_tag # will be removed at 2017/1/1
    register_tag_processor # will be removed at 2017/1/1
    
    # Others
    build_query("select * from {= title}"); # tag parsing functionality
                                            # will be removed at 2017/1/1

L<DBIx::Custom::Result>
    
    # Attribute methods
    filter_check # will be removed at 2017/1/1
    
    # Methods
    end_filter # will be removed at 2017/1/1
    remove_end_filter # will be removed at 2017/1/1
    remove_filter # will be removed at 2017/1/1
    default_filter # will be removed at 2017/1/1

L<DBIx::Custom::Tag>

    This module is DEPRECATED! # will be removed at 2017/1/1

=head1 BACKWARDS COMPATIBILITY POLICY

If a functionality is DEPRECATED, you can know it by DEPRECATED warnings
except for attribute method.
You can check all DEPRECATED functionalities by document.
DEPRECATED functionality is removed after five years,
but if at least one person use the functionality and tell me that thing
I extend one year each time he tell me it.

EXPERIMENTAL functionality will be changed without warnings.

This policy was changed at 2011/6/28

=head1 BUGS

Please tell me bugs if found.

C<< <kimoto.yuki at gmail.com> >>

L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009-2011 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
