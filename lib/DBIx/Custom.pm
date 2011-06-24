package DBIx::Custom;
use Object::Simple -base;

our $VERSION = '0.1697';
use 5.008001;

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder;
use DBIx::Custom::Where;
use DBIx::Custom::Model;
use DBIx::Custom::Tag;
use DBIx::Custom::Util qw/_array_to_hash _subname/;
use Encode qw/encode encode_utf8 decode_utf8/;

use constant DEBUG => $ENV{DBIX_CUSTOM_DEBUG} || 0;
use constant DEBUG_ENCODING => $ENV{DBIX_CUSTOM_DEBUG_ENCODING} || 'UTF-8';

our @COMMON_ARGS = qw/bind_type table query filter id primary_key
                      type_rule_off type_rule1_off type_rule2_off type/;

has [qw/connector dsn password quote user/],
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
    models => sub { {} },
    query_builder => sub { DBIx::Custom::QueryBuilder->new },
    result_class  => 'DBIx::Custom::Result',
    safety_character => '\w',
    stash => sub { {} };

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
    my $q = $self->_quote;
    foreach my $column (keys %$param) {
        croak qq{"$column" is not safety column name } . _subname
          unless $column =~ /^[$safety\.]+$/;
        my $column_quote = "$q$column$q";
        $column_quote =~ s/\./$q.$q/;
        push @params, "$column_quote = :$column";
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
    
    # Reserved word quote
    my $q = $self->_quote;
    
    # Separator
    my $separator = $self->separator;
    
    # Column clause
    my @column;
    $columns ||= [];
    push @column, "$q$table$q.$q$_$q as $q${table}${separator}$_$q"
      for @$columns;
    
    return join (', ', @column);
}

sub connect {
    my $self = ref $_[0] ? shift : shift->new(@_);;
    
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
            my $driver = $self->{dbh}->{Driver}->{Name};
            my $quote = $driver eq 'mysql' ? '`' : '"';
            $self->quote($quote);
        }
        
        return $self->{dbh};
    }
}

our %DELETE_ARGS = map { $_ => 1 } @COMMON_ARGS,
  qw/where append allow_delete_all where_param prefix/;

sub delete {
    my ($self, %args) = @_;

    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $DELETE_ARGS{$name};
    }
    
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
    my $q = $self->_quote;
    push @sql, "delete";
    push @sql, $prefix if defined $prefix;
    push @sql, "from $q$table$q $where_clause";
    push @sql, $append if defined $append;
    my $sql = join(' ', @sql);
    
    # Execute query
    return $self->execute($sql, $where_param, table => $table, %args);
}

sub delete_all { shift->delete(allow_delete_all => 1, @_) }

sub DESTROY { }

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
    $model->name($model_name) unless $model->name;
    $model->table($model_table) unless $model->table;
    
    # Apply filter
    my $filter = ref $model->filter eq 'HASH'
               ? [%{$model->filter}]
               : $model->filter;
    warn "DBIx::Custom::Model filter method is DEPRECATED!"
      if @$filter;
    $self->_apply_filter($model->table, @$filter);

    # Set model
    $self->model($model->name, $model);
    
    return $self->model($model->name);
}

sub each_column {
    my ($self, $cb) = @_;
    
    # Iterate all tables
    my $sth_tables = $self->dbh->table_info;
    while (my $table_info = $sth_tables->fetchrow_hashref) {
        
        # Table
        my $table = $table_info->{TABLE_NAME};
        
        # Iterate all columns
        my $sth_columns = $self->dbh->column_info(undef, undef, $table, '%');
        while (my $column_info = $sth_columns->fetchrow_hashref) {
            my $column = $column_info->{COLUMN_NAME};
            $self->$cb($table, $column, $column_info);
        }
    }
}

our %EXECUTE_ARGS = map { $_ => 1 } @COMMON_ARGS, 'param';

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
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $EXECUTE_ARGS{$name};
    }
    
    # Create query
    $query = $self->_create_query($query) unless ref $query;
    return $query if $query_return;
    $filter ||= $query->filter;
    
    # Tables
    unshift @$tables, @{$query->tables};
    my $main_table = pop @$tables;
    $tables = $self->_remove_duplicate_table($tables, $main_table);
    if (my $q = $self->_quote) {
        $_ =~ s/$q//g for @$tables;
    }
    
    # Type rule
    my $type_filters = {};
    unless ($type_rule_off) {
        foreach my $name (keys %$param) {
            my $table;
            my $column;
            if ($name =~ /(?:(.+)\.)?(.+)/) {
                $table = $1;
                $column = $2;
            }
            $table ||= $main_table;
            
            foreach my $i (1 .. 2) {
                unless ($type_rule_off_parts->{$i}) {
                    my $into = $self->{"_into$i"} || {};
                    if (defined $table && $into->{$table} &&
                        (my $rule = $into->{$table}->{$column}))
                    {
                        $type_filters->{$i}->{$column} = $rule;
                        $type_filters->{$i}->{"$table.$column"} = $rule;
                    }
                }
            }
        }
    }
    
    # Applied filter
    my $applied_filter = {};
    foreach my $table (@$tables) {
        $applied_filter = {
            %$applied_filter,
            %{$self->{filter}{out}->{$table} || {}}
        }
    }
    $filter = {%$applied_filter, %$filter};
    
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
    
    if ($@) {
        $self->_croak($@, qq{. Following SQL is executed.\n}
                        . qq{$query->{sql}\n} . _subname);
    }
    
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
        
        # Filter
        my $filter = {};
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
        
        # Result
        my $result = $self->result_class->new(
            sth => $sth,
            filters => $self->filters,
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

our %INSERT_ARGS = map { $_ => 1 } @COMMON_ARGS, qw/param/;

sub insert {
    my $self = shift;
    
    # Arguments
    my $param;
    $param = shift if @_ % 2;
    my %args = @_;
    my $table  = delete $args{table};
    croak qq{"table" option must be specified } . _subname
      unless $table;
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

    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $INSERT_ARGS{$name};
    }

    # Merge parameter
    if (defined $id) {
        my $id_param = $self->_create_param_from_id($id, $primary_key);
        $param = $self->merge_param($id_param, $param);
    }

    # Reserved word quote
    my $q = $self->_quote;
    
    # Insert statement
    my @sql;
    push @sql, "insert";
    push @sql, $prefix if defined $prefix;
    push @sql, "into $q$table$q " . $self->insert_param($param);
    push @sql, $append if defined $append;
    my $sql = join (' ', @sql);
    
    # Execute query
    return $self->execute($sql, $param, table => $table, %args);
}

sub insert_param {
    my ($self, $param) = @_;
    
    # Create insert parameter tag
    my $safety = $self->safety_character;
    my $q = $self->_quote;
    my @columns;
    my @placeholders;
    foreach my $column (keys %$param) {
        croak qq{"$column" is not safety column name } . _subname
          unless $column =~ /^[$safety\.]+$/;
        my $column_quote = "$q$column$q";
        $column_quote =~ s/\./$q.$q/;
        push @columns, $column_quote;
        push @placeholders, ":$column";
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
    my $q = $self->_quote;
    $columns ||= [];
    push @column, "$q$table$q.$q$_$q as $q$_$q" for @$columns;
    
    return join (', ', @column);
}

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attributes
    my @attrs = keys %$self;
    foreach my $attr (@attrs) {
        croak qq{"$attr" is wrong name } . _subname
          unless $self->can($attr);
    }
    
    # DEPRECATED!
    $self->query_builder->{tags} = {
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

sub not_exists { bless {}, 'DBIx::Custom::NotExists' }

sub register_filter {
    my $self = shift;
    
    # Register filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->filters({%{$self->filters}, %$filters});
    
    return $self;
}

our %SELECT_ARGS = map { $_ => 1 } @COMMON_ARGS,
  qw/column where relation join param where_param wrap prefix/;

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
    croak qq{"join" must be array reference } . _subname
      unless ref $join eq 'ARRAY';
    my $relation = delete $args{relation};
    warn "select() relation option is DEPRECATED! use join option instead"
      if $relation;
    my $param = delete $args{param} || {}; # DEPRECATED!
    warn "select() param option is DEPRECATED! use where_param option instead"
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
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $SELECT_ARGS{$name};
    }
    
    # Add relation tables(DEPRECATED!);
    $self->_add_relation_table($tables, $relation);
    
    # Select statement
    my @sql;
    push @sql, 'select';
    
    # Reserved word quote
    my $q = $self->_quote;
    
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
                croak "Format must be [COLUMN, as => ALIAS] " . _subname
                  unless @$column == 3 && $column->[1] eq 'as';
                $column = join(' ', $column->[0], 'as', $q . $column->[2] . $q);
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
            push @sql, ("$q$table$q", ',') unless $found->{$table};
            $found->{$table} = 1;
        }
    }
    else {
        my $main_table = $tables->[-1] || '';
        push @sql, "$q$main_table$q";
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

sub separator {
    my $self = shift;
    
    if (@_) {
        my $separator = $_[0] || '';
        croak qq{Separator must be "." or "__" or "-" } . _subname
          unless $separator eq '.' || $separator eq '__'
              || $separator eq '-';
        
        $self->{separator} = $separator;
    
        return $self;
    }
    return $self->{separator} ||= '.';
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

sub available_data_type {
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

sub available_type_name {
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

sub type_rule {
    my $self = shift;
    
    if (@_) {
        my $type_rule = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        # Into
        foreach my $i (1 .. 2) {
            my $into = "into$i";
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

                    $self->{"_$into"}{$table}{$column} = $filter;
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

our %UPDATE_ARGS = map { $_ => 1 } @COMMON_ARGS,
  qw/param where allow_update_all where_param prefix/;

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
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{"$name" is wrong option } . _subname
          unless $UPDATE_ARGS{$name};
    }

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
    my $q = $self->_quote;
    push @sql, "update";
    push @sql, $prefix if defined $prefix;
    push @sql, "$q$table$q $update_clause $where_clause";
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

sub where {
    my $self = shift;
    
    # Create where
    return DBIx::Custom::Where->new(
        query_builder => $self->query_builder,
        safety_character => $self->safety_character,
        quote => $self->_quote,
        @_
    );
}

sub _create_query {
    
    my ($self, $source) = @_;
    
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
            $query->filters($self->filters);
        }
    }
    
    # Create query
    unless ($query) {

        # Create query
        my $builder = $self->query_builder;
        $query = $builder->build_query($source);

        # Remove reserved word quote
        if (my $q = $self->_quote) {
            $_ =~ s/$q//g for @{$query->columns}
        }

        # Save query to cache
        $self->cache_method->(
            $self, $source,
            {
                sql     => $query->sql, 
                columns => $query->columns,
                tables  => $query->tables
            }
        ) if $cache;
    }
    
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
    $query->filters($self->filters);
    
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
            my $tf = $type_filter->{$column};
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
    warn "data_source is DEPRECATED! use dsn instead\n"
      if $dsn;
    $dsn ||= $self->dsn;
    croak qq{"dsn" must be specified } . _subname
      unless $dsn;
    my $user        = $self->user;
    my $password    = $self->password;
    my $dbi_option = {%{$self->dbi_options}, %{$self->dbi_option}};
    warn "dbi_options is DEPRECATED! use dbi_option instead\n"
      if keys %{$self->dbi_options};
    
    # Connect
    my $dbh = eval {DBI->connect(
        $dsn,
        $user,
        $password,
        {
            %{$self->default_dbi_option},
            %$dbi_option
        }
    )};
    
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
    my $q = $self->_quote;
    for (my $i = 0; $i < @$join; $i++) {
        
        # Search table in join clause
        my $join_clause = $join->[$i];
        my $q_re = quotemeta($q);
        my $join_re = $q ? qr/\s$q_re?([^\.\s$q_re]+?)$q_re?\..+?\s$q_re?([^\.\s$q_re]+?)$q_re?\..+?$/
                         : qr/\s([^\.\s]+?)\..+?\s([^\.\s]+?)\..+?$/;
        if ($join_clause =~ $join_re) {
            my $table1 = $1;
            my $table2 = $2;
            croak qq{right side table of "$join_clause" must be unique }
                . _subname
              if exists $tree->{$table2};
            $tree->{$table2}
              = {position => $i, parent => $table1, join => $join_clause};
        }
        else {
            croak qq{join clause must have two table name after "on" keyword. } .
                  qq{"$join_clause" is passed }  . _subname
        }
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

sub _remove_duplicate_table {
    my ($self, $tables, $main_table) = @_;
    
    # Remove duplicate table
    my %tables = map {defined $_ ? ($_ => 1) : ()} @$tables;
    delete $tables{$main_table} if $main_table;
    
    return [keys %tables, $main_table ? $main_table : ()];
}

sub _search_tables {
    my ($self, $source) = @_;
    
    # Search tables
    my $tables = [];
    my $safety_character = $self->safety_character;
    my $q = $self->_quote;
    my $q_re = quotemeta($q);
    my $table_re = $q ? qr/(?:^|[^$safety_character])$q_re?([$safety_character]+)$q_re?\./
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
            my $column_quote = "$q$column$q";
            $column_quote =~ s/\./$q.$q/;
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

# DEPRECATED!
sub _apply_filter {
    my ($self, $table, @cinfos) = @_;

    # Initialize filters
    $self->{filter} ||= {};
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
    
    warn "apply_filter is DEPRECATED! " . 
         "use type_rule method and DBIx::Custom::Result filter method, " .
         "instead";
    
    return $self->_apply_filter(@_);
}

# DEPRECATED!
our %SELECT_AT_ARGS = (%SELECT_ARGS, where => 1, primary_key => 1);
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
our %DELETE_AT_ARGS = (%DELETE_ARGS, where => 1, primary_key => 1);
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
our %UPDATE_AT_ARGS = (%UPDATE_ARGS, where => 1, primary_key => 1);
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
our %INSERT_AT_ARGS = (%INSERT_ARGS, where => 1, primary_key => 1);
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
    warn "register_tag is DEPRECATED!";
    shift->query_builder->register_tag(@_)
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
sub register_tag_processor {
    warn "register_tag_processor is DEPRECATED!";
    return shift->query_builder->register_tag_processor(@_);
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

DBIx::Custom - Useful database access, respecting SQL!

=head1 SYNOPSYS

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
    
=head1 DESCRIPTIONS

L<DBIx::Custom> is L<DBI> wrapper module.

=head1 FEATURES

L<DBIx::Custom> is the wrapper class of L<DBI> to execute SQL easily.
This module have the following features.

=over 4

=item * Execute INSERT, UPDATE, DELETE, SELECT statement easily

=item * You can specify bind values by hash reference

=item * Filtering by data type. and you can set filter to any column

=item * Creating where clause flexibly

=item * Support model

=back

=head1 GUIDE

L<DBIx::Custom::Guide> - L<DBIx::Custom> Guide

=head1 Wiki

L<DBIx::Custom Wiki|https://github.com/yuki-kimoto/DBIx-Custom/wiki>

=head1 ATTRIBUTES

=head2 C<connector>

    my $connector = $dbi->connector;
    $dbi = $dbi->connector(DBIx::Connector->new(...));

Connection manager object. if connector is set, you can get C<dbh>
through connection manager. conection manager object must have C<dbh> mehtod.

This is L<DBIx::Connector> example. Please pass
C<default_dbi_option> to L<DBIx::Connector> C<new> method.

    my $connector = DBIx::Connector->new(
        "dbi:mysql:database=$DATABASE",
        $USER,
        $PASSWORD,
        DBIx::Custom->new->default_dbi_option
    );
    
    my $dbi = DBIx::Custom->connect(connector => $connector);

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

=head2 C<models>

    my $models = $dbi->models;
    $dbi = $dbi->models(\%models);

Models, included by C<include_model> method.

=head2 C<password>

    my $password = $dbi->password;
    $dbi = $dbi->password('lkj&le`@s');

Password, used when C<connect> method is executed.

=head2 C<query_builder>

    my $sql_class = $dbi->query_builder;
    $dbi = $dbi->query_builder(DBIx::Custom::QueryBuilder->new);

Query builder, default to L<DBIx::Custom::QueryBuilder> object.

=head2 C<quote>

     my quote = $dbi->quote;
     $dbi = $dbi->quote('"');

Reserved word quote.
Default to double quote '"' except for mysql.
In mysql, default to back quote '`'

=head2 C<result_class>

    my $result_class = $dbi->result_class;
    $dbi = $dbi->result_class('DBIx::Custom::Result');

Result class, default to L<DBIx::Custom::Result>.

=head2 C<safety_character>

    my $safety_character = $self->safety_character;
    $dbi = $self->safety_character($character);

Regex of safety character for table and column name, default to '\w'.
Note that you don't have to specify like '[\w]'.

=head2 C<user>

    my $user = $dbi->user;
    $dbi = $dbi->user('Ken');

User name, used when C<connect> method is executed.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and use all methods of L<DBI>
and implements the following new ones.

=head2 C<available_data_type> EXPERIMENTAL

    print $dbi->available_data_type;

Get available data types. You can use these data types
in C<type rule>'s C<from1> and C<from2> section.

=head2 C<available_type_name> EXPERIMENTAL

    print $dbi->available_type_name;

Get available type names. You can use these type names in
C<type_rule>'s C<into1> and C<into2> section.

=head2 C<assign_param> EXPERIMENTAL

    my $assign_param = $dbi->assign_param({title => 'a', age => 2});

Create assign parameter.

    title = :title, author = :author

This is equal to C<update_param> exept that set is not added.

=head2 C<column> EXPERIMETNAL

    my $column = $dbi->column(book => ['author', 'title']);

Create column clause. The follwoing column clause is created.

    book.author as "book.author",
    book.title as "book.title"

You can change separator by C<separator> method.

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

Parameter is replaced by placeholder C<?>.

    select * from where title = ? and author like ?;

The following opitons are available.

=over 4

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
You can check executed SQL and columns order.

    my $sql = $query->sql;
    my $columns = $query->columns;

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

=item C<bind_type>

Specify database bind data type.

    bind_type => [image => DBI::SQL_BLOB]
    bind_type => [[qw/image audio/] => DBI::SQL_BLOB]

This is used to bind parameter by C<bind_param> of statment handle.

    $sth->bind_param($pos, $value, DBI::SQL_BLOB);

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

=item C<prefix> EXPERIMENTAL

    prefix => 'some'

prefix before table name section.

    delete some from book

=item C<query>

Same as C<execute> method's C<query> option.

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

=head2 C<insert>

    $dbi->insert({title => 'Perl', author => 'Ken'}, table  => 'book');

Execute insert statement. First argument is row data. Return value is
affected row count.

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

=item C<prefix> EXPERIMENTAL

    prefix => 'or replace'

prefix before table name section

    insert or replace into book

=item C<primary_key>

    primary_key => 'id'
    primary_key => ['id1', 'id2']

Primary key. This is used by C<id> option.

=item C<query>

Same as C<execute> method's C<query> option.

=item C<table>

    table => 'book'

Table name.

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

You get all type name used in database by C<available_type_name>.

    print $dbi->available_type_name;

In C<from1> and C<from2> you specify data type, not type name.
C<from2> is executed after C<from1>.
You get all data type by C<available_data_type>.

    print $dbi->available_data_type;

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
    
=item C<column>
    
    column => 'author'
    column => ['author', 'title']

Column clause.
    
if C<column> is not specified, '*' is set.

    column => '*'

You can specify hash of array reference. This is EXPERIMENTAL.

    column => [
        {book => [qw/author title/]},
        {person => [qw/name age/]}
    ]

This is expanded to the following one by using C<colomn> method.

    book.author as "book.author",
    book.title as "book.title",
    person.name as "person.name",
    person.age as "person.age"

You can specify array of array reference.

    column => [
        ['date(book.register_datetime)', as => 'book.register_date']
    ];

Alias is quoted and joined.

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

=itme C<prefix> EXPERIMENTAL

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

=item C<primary_key>

    primary_key => 'id'
    primary_key => ['id1', 'id2']

Primary key. This is used by C<id> option.

=item C<query>

Same as C<execute> method's C<query> option.

=item C<bind_type>

Same as C<execute> method's C<type> option.

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

    $dbi->select(wrap => ['select * from (', ') as t where ROWNUM < 10']);

This option is for Oracle and SQL Server paging process.

=back

=head2 C<update>

    $dbi->update({title => 'Perl'}, table  => 'book', where  => {id => 4});

Execute update statement. First argument is update data.

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

=item C<prefix> EXPERIMENTAL

    prefix => 'or replace'

prefix before table name section

    update or replace book

=item C<primary_key>

    primary_key => 'id'
    primary_key => ['id1', 'id2']

Primary key. This is used by C<id> option.

=item C<query>

Same as C<execute> method's C<query> option.

=item C<table>

    table => 'book'

Table name.

=item C<where>

Same as C<select> method's C<where> option.

=item C<bind_type>

Same as C<execute> method's C<type> option.

=item C<type_rule_off> EXPERIMENTAL

Same as C<execute> method's C<type_rule_off> option.

=item C<type_rule1_off> EXPERIMENTAL

    type_rule1_off => 1

Same as C<execute> method's C<type_rule1_off> option.

=item C<type_rule2_off> EXPERIMENTAL

    type_rule2_off => 1

Same as C<execute> method's C<type_rule2_off> option.

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

=head1 ENVIRONMENT VARIABLE

=head2 C<DBIX_CUSTOM_DEBUG>

If environment variable C<DBIX_CUSTOM_DEBUG> is set to true,
executed SQL and bind values are printed to STDERR.

=head2 C<DBIX_CUSTOM_DEBUG_ENCODING>

DEBUG output encoding. Default to UTF-8.

=head1 STABILITY

L<DBIx::Custom> is stable. APIs keep backword compatible
except EXPERIMENTAL one in the feature.

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
