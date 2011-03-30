package DBIx::Custom;

our $VERSION = '0.1667';

use 5.008001;
use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder;
use DBIx::Custom::Where;
use DBIx::Custom::Model;
use DBIx::Custom::Tag;
use DBIx::Custom::Util;
use Encode qw/encode_utf8 decode_utf8/;

our @COMMON_ARGS = qw/table query filter type/;

__PACKAGE__->attr(
    [qw/data_source password pid user/],
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
    reserved_word_quote => '',
    safety_character => '\w',
    stash => sub { {} }
);

our $AUTOLOAD;
sub AUTOLOAD {
    my $self = shift;

    # Method name
    my ($package, $mname) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

    # Method
    $self->{_methods} ||= {};
    if (my $method = $self->{_methods}->{$mname}) {
        return $self->$method(@_)
    }
    elsif (my $dbh_method = $self->dbh->can($mname)) {
        $self->dbh->$dbh_method(@_);
    }
    else {
        croak qq/Can't locate object method "$mname" via "$package"/
    }
}

sub apply_filter {
    my ($self, $table, @cinfos) = @_;

    # Initialize filters
    $self->{filter} ||= {};
    $self->{filter}{out} ||= {};
    $self->{filter}{in} ||= {};
    $self->{filter}{end} ||= {};
    
    # Create filters
    my $usage = "Usage: \$dbi->apply_filter(" .
                "TABLE, COLUMN1, {in => INFILTER1, out => OUTFILTER1, end => ENDFILTER1}, " .
                "COLUMN2, {in => INFILTER2, out => OUTFILTER2, end => ENDFILTER2}, ...)";

    for (my $i = 0; $i < @cinfos; $i += 2) {
        
        # Column
        my $column = $cinfos[$i];
        
        if (ref $column eq 'ARRAY') {
            foreach my $c (@$column) {
                push @cinfos, $c, $cinfos[$i + 1];
            }
            next;
        }
        
        # Filter info
        my $finfo = $cinfos[$i + 1] || {};
        croak "$usage (table: $table)" unless  ref $finfo eq 'HASH';
        foreach my $ftype (keys %$finfo) {
            croak "$usage (table: $table 2)" unless $ftype eq 'in' || $ftype eq 'out'
                             || $ftype eq 'end'; 
        }
        
        foreach my $way (qw/in out end/) {
            my $filter = $finfo->{$way};
            
            # State
            my $state = !exists $finfo->{$way} ? 'not_exists'
                      : !defined $filter        ? 'not_defined'
                      : ref $filter eq 'CODE'   ? 'code'
                      : 'name';
            
            next if $state eq 'not_exists';
            
            # Check filter
            croak qq{Filter "$filter" is not registered}
              if  $state eq 'name'
               && ! exists $self->filters->{$filter};
            
            # Filter
            my $f = $state eq 'not_defined' ? undef
                  : $state eq 'code'        ? $filter
                  : $self->filters->{$filter};
            $self->{filter}{$way}{$table}{$column} = $f;
            $self->{filter}{$way}{$table}{"$table.$column"} = $f;
            $self->{filter}{$way}{$table}{"${table}__$column"} = $f;
        }
    }
    
    return $self;
}

sub column {
    my ($self, $table, $columns) = @_;
    
    $columns ||= [];
    
    my $q = $self->reserved_word_quote;
    
    my @column;
    push @column, "$q$table$q.$q$_$q as $q${table}${q}__$q$_$q" for @$columns;
    
    return join (', ', @column);
}

sub connect {
    my $self = ref $_[0] ? shift : shift->new(@_);;
    
    my $dbh = $self->_connect;
    
    # Database handle
    $self->dbh($dbh);
    
    # Process ID
    $self->pid($$);
    
    return $self;
}

sub create_query {
    my ($self, $source) = @_;
    
    # Cache
    my $cache = $self->cache;
    
    # Create query
    my $query;
    if ($cache) {
        
        # Get query
        my $q = $self->cache_method->($self, $source);
        
        # Create query
        if ($q) {
            $query = DBIx::Custom::Query->new($q);
            $query->filters($self->filters);
        }
    }
    
    unless ($query) {

        # Create SQL object
        my $builder = $self->query_builder;
        
        # Create query
        $query = $builder->build_query($source);

        # Bind
        my $columns = $query->columns;
        if (my $q = $self->reserved_word_quote) {
            foreach my $column (@$columns) {
                $column =~ s/$q//g;
            }
        }

        # Cache query
        $self->cache_method->($self, $source,
                             {sql     => $query->sql, 
                              columns => $query->columns,
                              tables  => $query->tables})
          if $cache;
    }
    
    # Prepare statement handle
    my $sth;
    eval { $sth = $self->dbh->prepare($query->{sql})};
    $self->_croak($@, qq{. Following SQL is executed. "$query->{sql}"}) if $@;
    
    # Set statement handle
    $query->sth($sth);
    
    # Set filters
    $query->filters($self->filters);
    
    return $query;
}

sub dbh {
    my $self = shift;

    if (@_) {
        $self->{dbh} = $_[0];
        return $self;
    }
    else {
        my $pid = $$;
        if ($self->pid eq $pid) {
            return $self->{dbh};
        }
        else {
            # Create new connection in child process
            croak "Process is forked in transaction"
              unless $self->{dbh}->{AutoCommit};
            $self->pid($pid);
            $self->{dbh}->{InactiveDestroy} = 1;
            return $self->{dbh} = $self->_connect;
        }
    }
}

our %DELETE_ARGS
  = map { $_ => 1 } @COMMON_ARGS, qw/where append allow_delete_all/;

sub delete {
    my ($self, %args) = @_;

    # Quote for reserved word
    my $q = $self->reserved_word_quote;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $DELETE_ARGS{$name};
    }
    
    # Arguments
    my $table = $args{table} || '';
    croak qq{"table" option must be specified} unless $table;
    my $where            = delete $args{where} || {};
    my $append           = delete $args{append};
    my $allow_delete_all = delete $args{allow_delete_all};

    # Where
    my $w = $self->_where($where);
    $where = $w->param;
    
    # String where
    my $swhere = "$w";
    
    croak qq{"where" must be specified}
      if $swhere eq '' && !$allow_delete_all;

    # SQL stack
    my @sql;

    # Delete
    push @sql, "delete from $q$table$q $swhere";
    push @sql, $append if $append;
    
    my $sql = join(' ', @sql);
    
    # Create query
    my $query = $self->create_query($sql);
    return $query if $args{query};
    
    # Execute query
    my $ret_val = $self->execute(
        $query,
        param  => $where,
        table => $table,
        %args
    );
    
    return $ret_val;
}

sub delete_all { shift->delete(allow_delete_all => 1, @_) }

our %DELETE_AT_ARGS = (%DELETE_ARGS, where => 1, primary_key => 1);

sub delete_at {
    my ($self, %args) = @_;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $DELETE_AT_ARGS{$name};
    }
    
    # Primary key
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    
    # Where clause
    my $where = {};
    if (exists $args{where}) {
        my $where_columns = delete $args{where};
        $where_columns = [$where_columns] unless ref $where_columns;

        croak qq{"where" must be constant value or array reference}
          unless !ref $where_columns || ref $where_columns eq 'ARRAY';
        
        for(my $i = 0; $i < @$primary_keys; $i ++) {
           $where->{$primary_keys->[$i]} = $where_columns->[$i];
        }
    }
    
    if (exists $args{param}) {
        my $param = delete $args{param};
        
        for(my $i = 0; $i < @$primary_keys; $i ++) {
            delete $param->{$primary_keys->[$i]};
        }
    }
    
    return $self->delete(where => $where, %args);
}

sub DESTROY { }

our %EXECUTE_ARGS = map { $_ => 1 } @COMMON_ARGS, 'param';

sub create_model {
    my $self = shift;
    
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $args->{dbi} = $self;
    
    my $model_class = delete $args->{model_class} || 'DBIx::Custom::Model';
    my $model_name  = delete $args->{name};
    my $model_table = delete $args->{table};
    $model_name ||= $model_table;
    
    my $model = $model_class->new($args);
    $model->name($model_name) unless $model->name;
    $model->table($model_table) unless $model->table;
    
    # Apply filter
    croak "$model_class filter must be array reference"
      unless ref $model->filter eq 'ARRAY';
    $self->apply_filter($model->table, @{$model->filter});
    
    # Table - Model
    croak "Table name is duplicated"
      if exists $self->{_model_from}->{$model->table};
    $self->{_model_from}->{$model->table} = $model->name;

    # Table alias
    $self->{_table_alias} ||= {};
    $self->{_table_alias} = {%{$self->{_table_alias}}, %{$model->table_alias}};
    
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

sub execute{
    my ($self, $query, %args)  = @_;
    
    # Quote for reserved word
    my $q = $self->reserved_word_quote;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $EXECUTE_ARGS{$name};
    }
    
    my $params = $args{param} || {};
    
    # First argument is the soruce of SQL
    $query = $self->create_query($query)
      unless ref $query;
    
    # Applied filter
    my $filter = {};
    
    my $tables = $query->tables;
    if ($q) {
        foreach my $table (@$tables) {
            $table =~ s/$q//g;
        }
    }
    my $arg_tables = $args{table} || [];
    $arg_tables = [$arg_tables]
      unless ref $arg_tables eq 'ARRAY';
    push @$tables, @$arg_tables;

    # Organize tables
    my %table_set = map {defined $_ ? ($_ => 1) : ()} @$tables;
    my $main_table = pop @$tables;
    delete $table_set{$main_table} if $main_table;
    foreach my $table (keys %table_set) {
        push @$tables, $table;
        
        if (my $dist = $self->{_table_alias}->{$table}) {
            $self->{filter} ||= {};
            
            unless ($self->{filter}{out}{$table}) {
                $self->{filter}{out} ||= {};
                $self->{filter}{in}  ||= {};
                $self->{filter}{end} ||= {};
                
                foreach my $type (qw/out in end/) {
                    
                    foreach my $filter_name (keys %{$self->{filter}{$type}{$dist} || {}}) {
                        my $filter_name_alias = $filter_name;
                        $filter_name_alias =~ s/^$dist\./$table\./;
                        $filter_name_alias =~ s/^${dist}__/${table}__/; 
                        
                        $self->{filter}{$type}{$table}{$filter_name_alias}
                          = $self->{filter}{$type}{$dist}{$filter_name}
                    }
                }
            }
        }
    }
    
    $tables = [keys %table_set];
    push @$tables, $main_table if $main_table;
    
    foreach my $table (@$tables) {
        next unless $table;
        $filter = {
            %$filter,
            %{$self->{filter}{out}->{$table} || {}}
        }
    }
    
    # Filter argument
    my $f = DBIx::Custom::Util::array_to_hash($args{filter})
         || $query->filter || {};
    foreach my $column (keys %$f) {
        my $fname = $f->{$column};
        if (!defined $fname) {
            $f->{$column} = undef;
        }
        elsif (ref $fname ne 'CODE') {
          croak qq{Filter "$fname" is not registered"}
            unless exists $self->filters->{$fname};
          
          $f->{$column} = $self->filters->{$fname};
        }
    }
    $filter = {%$filter, %$f};
    
    # Type
    my $type = DBIx::Custom::Util::array_to_hash($args{type});
    
    # Bind
    my $bind = $self->_bind($params, $query->columns, $filter, $type);
    
    # Execute
    my $sth = $query->sth;
    my $affected;
    eval {
        for (my $i = 0; $i < @$bind; $i++) {
            if (my $type = $bind->[$i]->{type}) {
                $sth->bind_param($i + 1, $bind->[$i]->{value}, $type);
            }
            else {
                $sth->bind_param($i + 1, $bind->[$i]->{value});
            }
        }
        $affected = $sth->execute;
    };
    $self->_croak($@, qq{. Following SQL is executed. "$query->{sql}"}) if $@;
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        
        # Result in and end filter
        my $in_filter  = {};
        my $end_filter = {};
        foreach my $table (@$tables) {
            next unless $table;
            $in_filter = {
                %$in_filter,
                %{$self->{filter}{in}{$table} || {}}
            };
            $end_filter = {
                %$end_filter,
                %{$self->{filter}{end}{$table} || {}}
            };
        }
        
        # Result
        my $result = $self->result_class->new(
            sth            => $sth,
            filters        => $self->filters,
            filter_check   => $self->filter_check,
            default_filter => $self->{default_in_filter},
            filter         => $in_filter || {},
            end_filter     => $end_filter || {}
        );

        return $result;
    }
    return $affected;
}

our %INSERT_ARGS = map { $_ => 1 } @COMMON_ARGS, qw/param append/;

sub insert {
    my ($self, %args) = @_;
    
    # Quote for reserved word
    my $q = $self->reserved_word_quote;

    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $INSERT_ARGS{$name};
    }
    
    # Arguments
    my $table  = delete $args{table};
    croak qq{"table" option must be specified} unless $table;
    my $param  = delete $args{param} || {};
    my $append = delete $args{append} || '';
    
    # Columns
    my @columns;
    my $safety = $self->safety_character;
    foreach my $column (keys %$param) {
        croak qq{"$column" is not safety column name}
          unless $column =~ /^[$safety\.]+$/;
          $column = "$q$column$q";
          $column =~ s/\./$q.$q/;
        push @columns, $column;
    }
    
    # SQL stack
    my @sql;
    
    # Insert
    push @sql, "insert into $q$table$q {insert_param ". join(' ', @columns) . '}';
    push @sql, $append if $append;
    
    # SQL
    my $sql = join (' ', @sql);
    
    # Create query
    my $query = $self->create_query($sql);
    return $query if $args{query};
    
    # Execute query
    my $ret_val = $self->execute(
        $query,
        param  => $param,
        table => $table,
        %args
    );
    
    return $ret_val;
}

our %INSERT_AT_ARGS = (%INSERT_ARGS, where => 1, primary_key => 1);

sub insert_at {
    my ($self, %args) = @_;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $INSERT_AT_ARGS{$name};
    }
    
    # Primary key
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    
    # Where clause
    my $where = {};
    my $param = {};
    
    if (exists $args{where}) {
        my $where_columns = delete $args{where};
        $where_columns = [$where_columns] unless ref $where_columns;

        croak qq{"where" must be constant value or array reference}
          unless !ref $where_columns || ref $where_columns eq 'ARRAY';
        
        for(my $i = 0; $i < @$primary_keys; $i ++) {
           $where->{$primary_keys->[$i]} = $where_columns->[$i];
        }
    }
    
    if (exists $args{param}) {
        $param = delete $args{param};
        for(my $i = 0; $i < @$primary_keys; $i ++) {
             delete $param->{$primary_keys->[$i]};
        }
    }
    
    $param = {%$param, %$where};
    
    return $self->insert(param => $param, %args);
}

sub insert_param {
    my ($self, $param) = @_;
    
    # Insert parameter tag
    my @tag;
    push @tag, '{insert_param';
    my $safety = $self->safety_character;
    foreach my $column (keys %$param) {
        croak qq{"$column" is not safety column name}
          unless $column =~ /^[$safety\.]+$/;
        push @tag, $column;
    }
    push @tag, '}';
    
    return join ' ', @tag;
}

sub include_model {
    my ($self, $name_space, $model_infos) = @_;
    
    $name_space ||= '';
    unless ($model_infos) {
        # Load name space module
        croak qq{"$name_space" is invalid class name}
          if $name_space =~ /[^\w:]/;
        eval "use $name_space";
        croak qq{Name space module "$name_space.pm" is needed. $@} if $@;
        
        # Search model modules
        my $path = $INC{"$name_space.pm"};
        $path =~ s/\.pm$//;
        opendir my $dh, $path
          or croak qq{Can't open directory "$path": $!};
        $model_infos = [];
        while (my $module = readdir $dh) {
            push @$model_infos, $module
              if $module =~ s/\.pm$//;
        }
        
        close $dh;
    }
    
    foreach my $model_info (@$model_infos) {
        
        # Model class, name, table
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
        
        # Load
        croak qq{"$mclass" is invalid class name}
          if $mclass =~ /[^\w:]/;
        unless ($mclass->can('isa')) {
            eval "use $mclass";
            croak $@ if $@;
        }
        
        # Instantiate
        my $args = {};
        $args->{model_class} = $mclass if $mclass;
        $args->{name}        = $model_name if $model_name;
        $args->{table}       = $model_table if $model_table;
        
        # Create model
        $self->create_model($args);
    }
    
    return $self;
}

sub method {
    my $self = shift;
    
    # Merge
    my $methods = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_methods} = {%{$self->{_methods} || {}}, %$methods};
    
    return $self;
}

sub model {
    my ($self, $name, $model) = @_;
    
    # Set
    if ($model) {
        $self->models->{$name} = $model;
        return $self;
    }
    
    # Check model existance
    croak qq{Model "$name" is not included}
      unless $self->models->{$name};
    
    # Get
    return $self->models->{$name};
}

sub mycolumn {
    my ($self, $table, $columns) = @_;
    
    my $q = $self->reserved_word_quote;
    
    $columns ||= [];
    my @column;
    push @column, "$q$table$q.$q$_$q as $q$_$q" for @$columns;
    
    return join (', ', @column);
}

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attribute names
    my @attrs = keys %$self;
    foreach my $attr (@attrs) {
        croak qq{"$attr" is invalid attribute name}
          unless $self->can($attr);
    }

    $self->register_tag(
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
    );
    
    return $self;
}

sub not_exists { bless {}, 'DBIx::Custom::NotExists' }

sub register_filter {
    my $invocant = shift;
    
    # Register filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

sub register_tag { shift->query_builder->register_tag(@_) }

our %SELECT_ARGS
  = map { $_ => 1 } @COMMON_ARGS, qw/column where append relation join/;

sub select {
    my ($self, %args) = @_;

    # Quote for reserved word
    my $q = $self->reserved_word_quote;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $SELECT_ARGS{$name};
    }
    
    # Arguments
    my $table = delete $args{table};
    my $tables = ref $table eq 'ARRAY' ? $table
               : defined $table ? [$table]
               : [];
    my $columns   = delete $args{column};
    my $where     = delete $args{where} || {};
    my $append    = delete $args{append};
    my $join      = delete $args{join} || [];
    croak qq{"join" must be array reference}
      unless ref $join eq 'ARRAY';
    my $relation = delete $args{relation};
    
    # Add relation tables(DEPRECATED!);
    $self->_add_relation_table($tables, $relation);
    
    # SQL stack
    my @sql;
    push @sql, 'select';
    
    if ($columns) {

        $columns = [$columns] if ! ref $columns;
        
        if (ref $columns eq 'HASH') {
            # Find tables
            my $main_table;
            my %tables;
            if ($columns->{table}) {
                foreach my $table (@{$columns->{table}}) {
                    if (($table || '') eq $tables->[-1]) {
                        $main_table = $table;
                    }
                    else {
                        $tables{$table} = 1;
                    }
                }
            }
            elsif ($columns->{all}) {
                $main_table = $tables->[-1] || '';
                foreach my $j (@$join) {
                    my $tables = $self->_tables($j);
                    foreach my $table (@$tables) {
                        $tables{$table} = 1;
                    }
                }
                delete $tables{$main_table};
            }
            
            push @sql, $columns->{prepend} if $columns->{prepend};
            
            # Column clause of main table
            if ($main_table) {
                push @sql, $self->model($main_table)->mycolumn;
                push @sql, ',';
            }
            
            # Column cluase of other tables
            foreach my $table (keys %tables) {
                unshift @$tables, $table;
                push @sql, $self->model($table)->column($table);
                push @sql, ',';
            }
            pop @sql if $sql[-1] eq ',';
        }
        else {
            foreach my $column (@$columns) {
                unshift @$tables, @{$self->_tables($column)};
                push @sql, ($column, ',');
            }
            pop @sql if $sql[-1] eq ',';
        }
    }
    
    # "*" is default
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
    
    # Main table
    croak "Not found table name" unless $tables->[-1];
    
    # Where
    my $w = $self->_where($where);
    $where = $w->param;
    
    # String where
    my $swhere = "$w";
    
    # Add table names in where clause
    unshift @$tables, @{$self->_tables($swhere)};
    
    # Push join
    $self->_push_join(\@sql, $join, $tables);
    
    # Add where clause
    push @sql, $swhere;
    
    # Relation(DEPRECATED!);
    $self->_push_relation(\@sql, $tables, $relation, $swhere eq '' ? 1 : 0);
    
    # Append statement
    push @sql, $append if $append;
    
    # SQL
    my $sql = join (' ', @sql);
    
    # Create query
    my $query = $self->create_query($sql);
    return $query if $args{query};
    
    # Execute query
    my $result = $self->execute(
        $query,
        param  => $where, 
        table => $tables,
        %args
    );
    
    return $result;
}

our %SELECT_AT_ARGS = (%SELECT_ARGS, where => 1, primary_key => 1);

sub select_at {
    my ($self, %args) = @_;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $SELECT_AT_ARGS{$name};
    }
    
    # Primary key
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    
    # Table
    croak qq{"table" option must be specified} unless $args{table};
    my $table = ref $args{table} ? $args{table}->[-1] : $args{table};
    
    # Where clause
    my $where = {};
    if (exists $args{where}) {
        my $where_columns = delete $args{where};
        
        croak qq{"where" must be constant value or array reference}
          unless !ref $where_columns || ref $where_columns eq 'ARRAY';
        
        $where_columns = [$where_columns] unless ref $where_columns;
        
        for(my $i = 0; $i < @$primary_keys; $i ++) {
           $where->{$table . '.' . $primary_keys->[$i]} = $where_columns->[$i];
        }
    }
    
    if (exists $args{param}) {
        my $param = delete $args{param};
        for(my $i = 0; $i < @$primary_keys; $i ++) {
             delete $param->{$primary_keys->[$i]};
        }
    }
    
    return $self->select(where => $where, %args);
}

sub setup_model {
    my $self = shift;
    
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

our %UPDATE_ARGS
  = map { $_ => 1 } @COMMON_ARGS, qw/param where append allow_update_all/;

sub update {
    my ($self, %args) = @_;

    # Quote for reserved word
    my $q = $self->reserved_word_quote;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $UPDATE_ARGS{$name};
    }
    
    # Arguments
    my $table = delete $args{table} || '';
    croak qq{"table" option must be specified} unless $table;
    my $param            = delete $args{param} || {};
    my $where            = delete $args{where} || {};
    my $append           = delete $args{append} || '';
    my $allow_update_all = delete $args{allow_update_all};
    
    # Columns
    my @columns;
    my $safety = $self->safety_character;
    foreach my $column (keys %$param) {
        croak qq{"$column" is not safety column name}
          unless $column =~ /^[$safety\.]+$/;
          $column = "$q$column$q";
          $column =~ s/\./$q.$q/;
        push @columns, "$column";
    }
        
    # Update clause
    my $update_clause = '{update_param ' . join(' ', @columns) . '}';

    # Where
    my $w = $self->_where($where);
    $where = $w->param;
    
    # String where
    my $swhere = "$w";
    
    croak qq{"where" must be specified}
      if "$swhere" eq '' && !$allow_update_all;
    
    # SQL stack
    my @sql;
    
    # Update
    push @sql, "update $q$table$q $update_clause $swhere";
    push @sql, $append if $append;
    
    # Rearrange parameters
    foreach my $wkey (keys %$where) {
        
        if (exists $param->{$wkey}) {
            $param->{$wkey} = [$param->{$wkey}]
              unless ref $param->{$wkey} eq 'ARRAY';
            
            push @{$param->{$wkey}}, $where->{$wkey};
        }
        else {
            $param->{$wkey} = $where->{$wkey};
        }
    }
    
    # SQL
    my $sql = join(' ', @sql);
    
    # Create query
    my $query = $self->create_query($sql);
    return $query if $args{query};
    
    # Execute query
    my $ret_val = $self->execute(
        $query,
        param  => $param, 
        table => $table,
        %args
    );
    
    return $ret_val;
}

sub update_all { shift->update(allow_update_all => 1, @_) };

our %UPDATE_AT_ARGS = (%UPDATE_ARGS, where => 1, primary_key => 1);

sub update_at {
    my ($self, %args) = @_;
    
    # Check argument names
    foreach my $name (keys %args) {
        croak qq{Argument "$name" is invalid name}
          unless $UPDATE_AT_ARGS{$name};
    }
    
    # Primary key
    my $primary_keys = delete $args{primary_key};
    $primary_keys = [$primary_keys] unless ref $primary_keys;
    
    # Where clause
    my $where = {};
    my $param = {};
    
    if (exists $args{where}) {
        my $where_columns = delete $args{where};
        $where_columns = [$where_columns] unless ref $where_columns;

        croak qq{"where" must be constant value or array reference}
          unless !ref $where_columns || ref $where_columns eq 'ARRAY';
        
        for(my $i = 0; $i < @$primary_keys; $i ++) {
           $where->{$primary_keys->[$i]} = $where_columns->[$i];
        }
    }
    
    if (exists $args{param}) {
        $param = delete $args{param};
        for(my $i = 0; $i < @$primary_keys; $i ++) {
            delete $param->{$primary_keys->[$i]};
        }
    }
    
    return $self->update(where => $where, param => $param, %args);
}

sub update_param {
    my ($self, $param) = @_;
    
    # Update parameter tag
    my @tag;
    push @tag, '{update_param';
    my $safety = $self->safety_character;
    foreach my $column (keys %$param) {
        croak qq{"$column" is not safety column name}
          unless $column =~ /^[$safety\.]+$/;
        push @tag, $column;
    }
    push @tag, '}';
    
    return join ' ', @tag;
}

sub where {
    my $self = shift;

    return DBIx::Custom::Where->new(
        query_builder => $self->query_builder,
        safety_character => $self->safety_character,
        reserved_word_quote => $self->reserved_word_quote,
        @_
    );
}

sub _bind {
    my ($self, $params, $columns, $filter, $type) = @_;
    
    # bind values
    my $bind = [];
    
    # Build bind values
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
        
        # Type
        push @$bind, {
            value => $f ? $f->($value) : $value,
            type => $type->{$column}
        };
        
        # Count up 
        $count->{$column}++;
    }
    
    return $bind;
}

sub _connect {
    my $self = shift;
    
    # Attributes
    my $data_source = $self->data_source;
    croak qq{"data_source" must be specified to connect()"}
      unless $data_source;
    my $user        = $self->user;
    my $password    = $self->password;
    my $dbi_option = {%{$self->dbi_options}, %{$self->dbi_option}};
    
    # Connect
    my $dbh = eval {DBI->connect(
        $data_source,
        $user,
        $password,
        {
            %{$self->default_dbi_option},
            %$dbi_option
        }
    )};
    
    # Connect error
    croak $@ if $@;
    
    return $dbh;
}

sub _croak {
    my ($self, $error, $append) = @_;
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
    
    foreach my $table (@$tables) {
        
        if ($tree->{$table}) {
            $need_tables->{$table} = 1;
            $self->_need_tables($tree, $need_tables, [$tree->{$table}{parent}])
        }
    }
}

sub _tables {
    my ($self, $source) = @_;
    
    my $tables = [];
    
    my $safety_character = $self->safety_character;
    my $q = $self->reserved_word_quote;
    my $q_re = quotemeta($q);
    
    my $table_re = $q ? qr/\b$q_re?([$safety_character]+)$q_re?\./
                      : qr/\b([$safety_character]+)\./;
    while ($source =~ /$table_re/g) {
        push @$tables, $1;
    }
    
    return $tables;
}

sub _push_join {
    my ($self, $sql, $join, $join_tables) = @_;
    
    return unless @$join;
    
    my $q = $self->reserved_word_quote;
    
    my $tree = {};
    
    for (my $i = 0; $i < @$join; $i++) {
        
        my $join_clause = $join->[$i];
        my $q_re = quotemeta($q);
        my $join_re = $q ? qr/\s$q_re?([^\.\s$q_re]+?)$q_re?\..+\s$q_re?([^\.\s$q_re]+?)$q_re?\..+?$/
                         : qr/\s([^\.\s]+?)\..+\s([^\.\s]+?)\..+?$/;
        if ($join_clause =~ $join_re) {
            
            my $table1 = $1;
            my $table2 = $2;
            
            croak qq{right side table of "$join_clause" must be uniq}
              if exists $tree->{$table2};
            
            $tree->{$table2}
              = {position => $i, parent => $table1, join => $join_clause};
        }
        else {
            croak qq{join "$join_clause" must be two table name};
        }
    }
    
    my $need_tables = {};
    $self->_need_tables($tree, $need_tables, $join_tables);
    
    my @need_tables = sort { $tree->{$a}{position} <=> $tree->{$b}{position} } keys %$need_tables;

    foreach my $need_table (@need_tables) {
        push @$sql, $tree->{$need_table}{join};
    }
}

sub _where {
    my ($self, $where) = @_;
    
    my $w;
    if (ref $where eq 'HASH') {
        my $clause = ['and'];
        my $q = $self->reserved_word_quote;
        foreach my $column (keys %$where) {
            $column = "$q$column$q";
            $column =~ s/\./$q.$q/;
            push @$clause, "{= $column}" for keys %$where;
        }
        
        $w = $self->where(clause => $clause, param => $where);
    }
    elsif (ref $where eq 'DBIx::Custom::Where') {
        $w = $where;
    }
    elsif (ref $where eq 'ARRAY') {
        $w = $self->where(
            clause => $where->[0],
            param  => $where->[1]
        );
    }
    
    croak qq{"where" must be hash reference or DBIx::Custom::Where object} .
          qq{or array reference, which contains where clause and paramter}
      unless ref $w eq 'DBIx::Custom::Where';
    
    return $w;
}

# DEPRECATED!
__PACKAGE__->attr(
    dbi_options => sub { {} },
    filter_check  => 1
);

# DEPRECATED!
sub default_bind_filter {
    my $self = shift;
    
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
sub register_tag_processor {
    return shift->query_builder->register_tag_processor(@_);
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
        data_source => "dbi:mysql:database=dbname",
        user => 'ken',
        password => '!LFKD%$&',
        dbi_option => {mysql_enable_utf8 => 1}
    );

    # Insert 
    $dbi->insert(
        table  => 'book',
        param  => {title => 'Perl', author => 'Ken'}
    );
    
    # Update 
    $dbi->update(
        table  => 'book', 
        param  => {title => 'Perl', author => 'Ken'}, 
        where  => {id => 5},
    );
    
    # Delete
    $dbi->delete(
        table  => 'book',
        where  => {author => 'Ken'},
    );

    # Select
    my $result = $dbi->select(
        table  => 'book',
        where  => {author => 'Ken'},
    );

    # Select, more complex
    my $result = $dbi->select(
        table  => 'book',
        column => [
            'book.author as book__author',
            'company.name as company__name'
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
        "select id from book where {= author} and {like title}",
        param  => {author => 'ken', title => '%Perl%'}
    );
    
=head1 DESCRIPTIONS

L<DBIx::Custom> is L<DBI> wrapper module.

=head1 FEATURES

=over 4

=item *

There are many basic methods to execute various queries.
C<insert()>, C<update()>, C<update_all()>,C<delete()>,
C<delete_all()>, C<select()>,
C<insert_at()>, C<update_at()>, 
C<delete_at()>, C<select_at()>, C<execute()>

=item *

Filter when data is send or receive.

=item *

Data filtering system

=item *

Model support.

=item *

Generate where clause dinamically.

=item *

Generate join clause dinamically.

=back

=head1 GUIDE

L<DBIx::Custom::Guide> - L<DBIx::Custom> Guide

=head1 Wiki

L<DBIx::Custom Wiki|https://github.com/yuki-kimoto/DBIx-Custom/wiki>

=head1 ATTRIBUTES

=head2 C<data_source>

    my $data_source = $dbi->data_source;
    $dbi            = $dbi->data_source("DBI:mysql:database=dbname");

Data source, used when C<connect()> is executed.

=head2 C<dbi_option>

    my $dbi_option = $dbi->dbi_option;
    $dbi           = $dbi->dbi_option($dbi_option);

L<DBI> option, used when C<connect()> is executed.
Each value in option override the value of C<default_dbi_option>.

=head2 C<default_dbi_option>

    my $default_dbi_option = $dbi->default_dbi_option;
    $dbi            = $dbi->default_dbi_option($default_dbi_option);

L<DBI> default option, used when C<connect()> is executed,
default to the following values.

    {
        RaiseError => 1,
        PrintError => 0,
        AutoCommit => 1,
    }

You should not change C<AutoCommit> value directly,
the value is used to check if the process is in transaction.

=head2 C<filters>

    my $filters = $dbi->filters;
    $dbi        = $dbi->filters(\%filters);

Filters, registered by C<register_filter()>.

=head2 C<models> EXPERIMENTAL

    my $models = $dbi->models;
    $dbi       = $dbi->models(\%models);

Models, included by C<include_model()>.

=head2 C<password>

    my $password = $dbi->password;
    $dbi         = $dbi->password('lkj&le`@s');

Password, used when C<connect()> is executed.

=head2 C<query_builder>

    my $sql_class = $dbi->query_builder;
    $dbi          = $dbi->query_builder(DBIx::Custom::QueryBuilder->new);

Query builder, default to L<DBIx::Custom::QueryBuilder> object.

=head2 C<reserved_word_quote> EXPERIMENTAL

     my reserved_word_quote = $dbi->reserved_word_quote;
     $dbi                   = $dbi->reserved_word_quote('"');

Quote for reserved word, default to empty string.

=head2 C<result_class>

    my $result_class = $dbi->result_class;
    $dbi             = $dbi->result_class('DBIx::Custom::Result');

Result class, default to L<DBIx::Custom::Result>.

=head2 C<safety_character>

    my $safety_character = $self->safety_character;
    $dbi                 = $self->safety_character($character);

Regex of safety character for table and column name, default to '\w'.
Note that you don't have to specify like '[\w]'.

=head2 C<user>

    my $user = $dbi->user;
    $dbi     = $dbi->user('Ken');

User name, used when C<connect()> is executed.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and use all methods of L<DBI>
and implements the following new ones.

=head2 C<apply_filter> EXPERIMENTAL

    $dbi->apply_filter(
        'book',
        'issue_date' => {
            out => 'tp_to_date',
            in  => 'date_to_tp',
            end => 'tp_to_displaydate'
        },
        'write_date' => {
            out => 'tp_to_date',
            in  => 'date_to_tp',
            end => 'tp_to_displaydate'
        }
    );

Apply filter to columns.
C<out> filter is executed before data is send to database.
C<in> filter is executed after a row is fetch.
C<end> filter is execute after C<in> filter is executed.

Filter is applied to the follwoing tree column name pattern.

       PETTERN         EXAMPLE
    1. Column        : author
    2. Table.Column  : book.author
    3. Table__Column : book__author

If column name is duplicate with other table,
Main filter specified by C<table> option is used.

You can set multiple filters at once.

    $dbi->apply_filter(
        'book',
        [qw/issue_date write_date/] => {
            out => 'tp_to_date',
            in  => 'date_to_tp',
            end => 'tp_to_displaydate'
        }
    );

=head2 C<connect>

    my $dbi = DBIx::Custom->connect(
        data_source => "dbi:mysql:database=dbname",
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
        filter => [
            publish_date => {
                out => 'tp_to_date',
                in => 'date_to_tp',
                end => 'tp_to_displaydate'
            }
        ]
    );

Create L<DBIx::Custom::Model> object and initialize model.
the module is also used from model() method.

   $dbi->model('book')->select(...);

=head2 C<create_query>
    
    my $query = $dbi->create_query(
        "insert into book {insert_param title author};";
    );

Create L<DBIx::Custom::Query> object.

If you want to get high performance,
create L<DBIx::Custom::Query> object and execute the query by C<execute()>
instead of other methods, such as C<insert>, C<update>.

    $dbi->execute($query, {author => 'Ken', title => '%Perl%'});

=head2 C<dbh>

    my $dbh = $dbi->dbh;
    $dbi    = $dbi->dbh($dbh);

Get and set database handle of L<DBI>.

If process is spawn by forking, new connection is created automatically.

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
        "select * from book where {= title} and {like author}",
        param => {title => 'Perl', author => '%Ken%'}
    );

Execute SQL, containing tags.
Return value is L<DBIx::Custom::Result> in select statement, or
the count of affected rows in insert, update, delete statement.

Tag is turned into the statement containing place holder
before SQL is executed.

    select * from where title = ? and author like ?;

See also L<Tags/Tags>.

The following opitons are currently available.

=over 4

=item C<filter>

Filter, executed before data is send to database. This is array reference.
Filter value is code reference or
filter name registerd by C<register_filter()>.

    # Basic
    $dbi->execute(
        $sql,
        filter => [
            title  => sub { uc $_[0] }
            author => sub { uc $_[0] }
        ]
    );
    
    # At once
    $dbi->execute(
        $sql,
        filter => [
            [qw/title author/]  => sub { uc $_[0] }
        ]
    );
    
    # Filter name
    $dbi->execute(
        $sql,
        filter => [
            title  => 'upper_case',
            author => 'upper_case'
        ]
    );

These filters are added to the C<out> filters, set by C<apply_filter()>.

=back

=head2 C<delete>

    $dbi->delete(table => 'book', where => {title => 'Perl'});

Delete statement.

The following opitons are currently available.

=over 4

=item C<table>

Table name.

    $dbi->delete(table => 'book');

=item C<where>

Where clause. This is hash reference or L<DBIx::Custom::Where> object
or array refrence, which contains where clause and paramter.
    
    # Hash reference
    $dbi->delete(where => {title => 'Perl'});
    
    # DBIx::Custom::Where object
    my $where = $dbi->where(
        clause => ['and', '{= author}', '{like title}'],
        param  => {author => 'Ken', title => '%Perl%'}
    );
    $dbi->delete(where => $where);

    # Array refrendce (where clause and parameter)
    $dbi->delete(where =>
        [
            ['and', '{= author}', '{like title}'],
            {author => 'Ken', title => '%Perl%'}
        ]
    );
    
=item C<append>

Append statement to last of SQL. This is string.

    $dbi->delete(append => 'order by title');

=item C<filter>

Filter, executed before data is send to database. This is array reference.
Filter value is code reference or
filter name registerd by C<register_filter()>.

    # Basic
    $dbi->delete(
        filter => [
            title  => sub { uc $_[0] }
            author => sub { uc $_[0] }
        ]
    );
    
    # At once
    $dbi->delete(
        filter => [
            [qw/title author/]  => sub { uc $_[0] }
        ]
    );
    
    # Filter name
    $dbi->delete(
        filter => [
            title  => 'upper_case',
            author => 'upper_case'
        ]
    );

These filters are added to the C<out> filters, set by C<apply_filter()>.

=head2 C<column> EXPERIMENTAL

    my $column = $self->column(book => ['author', 'title']);

Create column clause. The follwoing column clause is created.

    book.author as book__author,
    book.title as book__title

=item C<query>

Get L<DBIx::Custom::Query> object instead of executing SQL.
This is true or false value.

    my $query = $dbi->delete(query => 1);

You can check SQL.

    my $sql = $query->sql;

=back

=head2 C<delete_all>

    $dbi->delete_all(table => $table);

Delete statement to delete all rows.
Options is same as C<delete()>.

=head2 C<delete_at()>

Delete statement, using primary key.

    $dbi->delete_at(
        table => 'book',
        primary_key => 'id',
        where => '5'
    );

This method is same as C<delete()> exept that
C<primary_key> is specified and C<where> is constant value or array refrence.
all option of C<delete()> is available.

=over 4

=item C<primary_key>

Primary key. This is constant value or array reference.
    
    # Constant value
    $dbi->delete(primary_key => 'id');

    # Array reference
    $dbi->delete(primary_key => ['id1', 'id2' ]);

This is used to create where clause.

=item C<where>

Where clause, created from primary key information.
This is constant value or array reference.

    # Constant value
    $dbi->delete(where => 5);

    # Array reference
    $dbi->delete(where => [3, 5]);

In first examle, the following SQL is created.

    delete from book where id = ?;

Place holder is set to 5.

=back

=head2 C<insert>

    $dbi->insert(
        table  => 'book', 
        param  => {title => 'Perl', author => 'Ken'}
    );

Insert statement.

The following opitons are currently available.

=over 4

=item C<table>

Table name.

    $dbi->insert(table => 'book');

=item C<param>

Insert data. This is hash reference.

    $dbi->insert(param => {title => 'Perl'});

=item C<append>

Append statement to last of SQL. This is string.

    $dbi->insert(append => 'order by title');

=item C<filter>

Filter, executed before data is send to database. This is array reference.
Filter value is code reference or
filter name registerd by C<register_filter()>.

    # Basic
    $dbi->insert(
        filter => [
            title  => sub { uc $_[0] }
            author => sub { uc $_[0] }
        ]
    );
    
    # At once
    $dbi->insert(
        filter => [
            [qw/title author/]  => sub { uc $_[0] }
        ]
    );
    
    # Filter name
    $dbi->insert(
        filter => [
            title  => 'upper_case',
            author => 'upper_case'
        ]
    );

These filters are added to the C<out> filters, set by C<apply_filter()>.

=item C<query>

Get L<DBIx::Custom::Query> object instead of executing SQL.
This is true or false value.

    my $query = $dbi->insert(query => 1);

You can check SQL.

    my $sql = $query->sql;

=back

=head2 C<insert_at()>

Insert statement, using primary key.

    $dbi->insert_at(
        table => 'book',
        primary_key => 'id',
        where => '5',
        param => {title => 'Perl'}
    );

This method is same as C<insert()> exept that
C<primary_key> is specified and C<where> is constant value or array refrence.
all option of C<insert()> is available.

=over 4

=item C<primary_key>

Primary key. This is constant value or array reference.
    
    # Constant value
    $dbi->insert(primary_key => 'id');

    # Array reference
    $dbi->insert(primary_key => ['id1', 'id2' ]);

This is used to create parts of insert data.

=item C<where>

Parts of Insert data, create from primary key information.
This is constant value or array reference.

    # Constant value
    $dbi->insert(where => 5);

    # Array reference
    $dbi->insert(where => [3, 5]);

In first examle, the following SQL is created.

    insert into book (id, title) values (?, ?);

Place holders are set to 5 and 'Perl'.

=back

=head2 C<insert_param>

    my $insert_param = $dbi->insert_param({title => 'a', age => 2});

Create insert parameter tag.

    {insert_param title age}

=head2 C<include_model> EXPERIMENTAL

    $dbi->include_model('MyModel');

Include models from specified namespace,
the following layout is needed to include models.

    lib / MyModel.pm
        / MyModel / book.pm
                  / company.pm

Name space module, extending L<DBIx::Custom::Model>.

B<MyModel.pm>

    package MyModel;
    
    use base 'DBIx::Custom::Model';
    
    1;

Model modules, extending name space module.

B<MyModel/book.pm>

    package MyModel::book;
    
    use base 'MyModel';
    
    1;

B<MyModel/company.pm>

    package MyModel::company;
    
    use base 'MyModel';
    
    1;
    
MyModel::book and MyModel::company is included by C<include_model()>.

You can get model object by C<model()>.

    my $book_model    = $dbi->model('book');
    my $company_model = $dbi->model('company');

See L<DBIx::Custom::Model> to know model features.

=head2 C<method> EXPERIMENTAL

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

=head2 C<model> EXPERIMENTAL

    $dbi->model('book')->method(
        insert => sub { ... },
        update => sub { ... }
    );
    
    my $model = $dbi->model('book');

Set and get a L<DBIx::Custom::Model> object,

=head2 C<mycolumn> EXPERIMENTAL

    my $column = $self->mycolumn(book => ['author', 'title']);

Create column clause for myself. The follwoing column clause is created.

    book.author as author,
    book.title as title

=head2 C<new>

    my $dbi = DBIx::Custom->new(
        data_source => "dbi:mysql:database=dbname",
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

=head2 C<register_tag>

    $dbi->register_tag(
        update => sub {
            my @columns = @_;
            
            # Update parameters
            my $s = 'set ';
            $s .= "$_ = ?, " for @columns;
            $s =~ s/, $//;
            
            return [$s, \@columns];
        }
    );

Register tag, used by C<execute()>.

See also L<Tags/Tags> about tag registered by default.

Tag parser receive arguments specified in tag.
In the following tag, 'title' and 'author' is parser arguments

    {update_param title author} 

Tag parser must return array refrence,
first element is the result statement, 
second element is column names corresponding to place holders.

In this example, result statement is 

    set title = ?, author = ?

Column names is

    ['title', 'author']

=head2 C<select>

    my $result = $dbi->select(
        table  => 'book',
        column => ['author', 'title'],
        where  => {author => 'Ken'},
    );
    
Select statement.

The following opitons are currently available.

=over 4

=item C<table>

Table name.

    $dbi->select(table => 'book');

=item C<column>

Column clause. This is array reference or constant value.

    # Hash refernce
    $dbi->select(column => ['author', 'title']);
    
    # Constant value
    $dbi->select(column => 'author');

Default is '*' unless C<column> is specified.

    # Default
    $dbi->select(column => '*');

You can use hash option in C<column>

=over 4

=item all EXPERIMENTAL

Colum clause, contains all columns of joined table. This is true or false value

    $dbi->select(column => {all => 1});

If main table is C<book> and joined table is C<company>,
This create the following column clause.

    book.author as author
    book.company_id as company_id
    company.id as company__id
    company.name as company__name

Columns of main table is consist of only column name,
Columns of joined table is consist of table and column name joined C<__>.

Note that this option is failed unless modles is included and
C<columns> attribute is set.

    # Generally do the following way before using all_column option
    $dbi->include_model('MyModel')->setup_model;

=item table EXPERIMENTAL

You can also specify table names by C<table> option

    $dbi->select(column => {table => ['book', 'company']});

=item prepend EXPERIMENTAL

You can add before created statement

    $dbi->select(column => {prepend => 'SOME', all => 1});

=back

=item C<where>

Where clause. This is hash reference or L<DBIx::Custom::Where> object,
or array refrence, which contains where clause and paramter.
    
    # Hash reference
    $dbi->select(where => {author => 'Ken', 'title' => 'Perl'});
    
    # DBIx::Custom::Where object
    my $where = $dbi->where(
        clause => ['and', '{= author}', '{like title}'],
        param  => {author => 'Ken', title => '%Perl%'}
    );
    $dbi->select(where => $where);

    # Array refrendce (where clause and parameter)
    $dbi->select(where =>
        [
            ['and', '{= author}', '{like title}'],
            {author => 'Ken', title => '%Perl%'}
        ]
    );
    
=item C<join> EXPERIMENTAL

Join clause used in need. This is array reference.

    $dbi->select(join =>
        [
            'left outer join company on book.company_id = company_id',
            'left outer join location on company.location_id = location.id'
        ]
    );

If column cluase or where clause contain table name like "company.name",
needed join clause is used automatically.

    $dbi->select(
        table => 'book',
        column => ['company.location_id as company__location_id'],
        where => {'company.name' => 'Orange'},
        join => [
            'left outer join company on book.company_id = company.id',
            'left outer join location on company.location_id = location.id'
        ]
    );

In above select, the following SQL is created.

    select company.location_id as company__location_id
    from book
      left outer join company on book.company_id = company.id
    where company.name = Orange

=item C<append>

Append statement to last of SQL. This is string.

    $dbi->select(append => 'order by title');

=item C<filter>

Filter, executed before data is send to database. This is array reference.
Filter value is code reference or
filter name registerd by C<register_filter()>.

    # Basic
    $dbi->select(
        filter => [
            title  => sub { uc $_[0] }
            author => sub { uc $_[0] }
        ]
    );
    
    # At once
    $dbi->select(
        filter => [
            [qw/title author/]  => sub { uc $_[0] }
        ]
    );
    
    # Filter name
    $dbi->select(
        filter => [
            title  => 'upper_case',
            author => 'upper_case'
        ]
    );

These filters are added to the C<out> filters, set by C<apply_filter()>.

=item C<query>

Get L<DBIx::Custom::Query> object instead of executing SQL.
This is true or false value.

    my $query = $dbi->select(query => 1);

You can check SQL.

    my $sql = $query->sql;

=item C<type> EXPERIMENTAL

Specify database data type.

    $dbi->select(type => [image => DBI::SQL_BLOB]);
    $dbi->select(type => [[qw/image audio/] => DBI::SQL_BLOB]);

This is used to bind paramter by C<bind_param()> of statment handle.

    $sth->bind_param($pos, $value, DBI::SQL_BLOB);

=back

=head2 C<select_at()>

Select statement, using primary key.

    $dbi->select_at(
        table => 'book',
        primary_key => 'id',
        where => '5'
    );

This method is same as C<select()> exept that
C<primary_key> is specified and C<where> is constant value or array refrence.
all option of C<select()> is available.

=over 4

=item C<primary_key>

Primary key. This is constant value or array reference.
    
    # Constant value
    $dbi->select(primary_key => 'id');

    # Array reference
    $dbi->select(primary_key => ['id1', 'id2' ]);

This is used to create where clause.

=item C<where>

Where clause, created from primary key information.
This is constant value or array reference.

    # Constant value
    $dbi->select(where => 5);

    # Array reference
    $dbi->select(where => [3, 5]);

In first examle, the following SQL is created.

    select * from book where id = ?

Place holder is set to 5.

=back

=head2 C<update>

    $dbi->update(
        table  => 'book',
        param  => {title => 'Perl'},
        where  => {id => 4}
    );

Update statement.

The following opitons are currently available.

=over 4

=item C<table>

Table name.

    $dbi->update(table => 'book');

=item C<param>

Update data. This is hash reference.

    $dbi->update(param => {title => 'Perl'});

=item C<where>

Where clause. This is hash reference or L<DBIx::Custom::Where> object
or array refrence.
    
    # Hash reference
    $dbi->update(where => {author => 'Ken', 'title' => 'Perl'});
    
    # DBIx::Custom::Where object
    my $where = $dbi->where(
        clause => ['and', '{= author}', '{like title}'],
        param  => {author => 'Ken', title => '%Perl%'}
    );
    $dbi->update(where => $where);
    
    # Array refrendce (where clause and parameter)
    $dbi->update(where =>
        [
            ['and', '{= author}', '{like title}'],
            {author => 'Ken', title => '%Perl%'}
        ]
    );

=item C<append>

Append statement to last of SQL. This is string.

    $dbi->update(append => 'order by title');

=item C<filter>

Filter, executed before data is send to database. This is array reference.
Filter value is code reference or
filter name registerd by C<register_filter()>.

    # Basic
    $dbi->update(
        filter => [
            title  => sub { uc $_[0] }
            author => sub { uc $_[0] }
        ]
    );
    
    # At once
    $dbi->update(
        filter => [
            [qw/title author/]  => sub { uc $_[0] }
        ]
    );
    
    # Filter name
    $dbi->update(
        filter => [
            title  => 'upper_case',
            author => 'upper_case'
        ]
    );

These filters are added to the C<out> filters, set by C<apply_filter()>.

=item C<query>

Get L<DBIx::Custom::Query> object instead of executing SQL.
This is true or false value.

    my $query = $dbi->update(query => 1);

You can check SQL.

    my $sql = $query->sql;

=back

=head2 C<update_all>

    $dbi->update_all(table => 'book', param => {title => 'Perl'});

Update statement to update all rows.
Options is same as C<update()>.

=head2 C<update_at()>

Update statement, using primary key.

    $dbi->update_at(
        table => 'book',
        primary_key => 'id',
        where => '5',
        param => {title => 'Perl'}
    );

This method is same as C<update()> exept that
C<primary_key> is specified and C<where> is constant value or array refrence.
all option of C<update()> is available.

=over 4

=item C<primary_key>

Primary key. This is constant value or array reference.
    
    # Constant value
    $dbi->update(primary_key => 'id');

    # Array reference
    $dbi->update(primary_key => ['id1', 'id2' ]);

This is used to create where clause.

=item C<where>

Where clause, created from primary key information.
This is constant value or array reference.

    # Constant value
    $dbi->update(where => 5);

    # Array reference
    $dbi->update(where => [3, 5]);

In first examle, the following SQL is created.

    update book set title = ? where id = ?

Place holders are set to 'Perl' and 5.

=back

=head2 C<update_param>

    my $update_param = $dbi->update_param({title => 'a', age => 2});

Create update parameter tag.

    {update_param title age}

=head2 C<where>

    my $where = $dbi->where(
        clause => ['and', '{= title}', '{= author}'],
        param => {title => 'Perl', author => 'Ken'}
    );

Create a new L<DBIx::Custom::Where> object.

=head2 C<setup_model> EXPERIMENTAL

    $dbi->setup_model;

Setup all model objects.
C<columns> of model object is automatically set, parsing database information.

=head1 Tags

The following tags is available.

=head2 C<table>

Table tag

    {table TABLE}    ->    TABLE

This is used to tell C<execute()> what table is needed .

=head2 C<?>

Placeholder tag.

    {? NAME}    ->   ?

=head2 C<=>

Equal tag.

    {= NAME}    ->   NAME = ?

=head2 C<E<lt>E<gt>>

Not equal tag.

    {<> NAME}   ->   NAME <> ?

=head2 C<E<lt>>

Lower than tag

    {< NAME}    ->   NAME < ?

=head2 C<E<gt>>

Greater than tag

    {> NAME}    ->   NAME > ?

=head2 C<E<gt>=>

Greater than or equal tag

    {>= NAME}   ->   NAME >= ?

=head2 C<E<lt>=>

Lower than or equal tag

    {<= NAME}   ->   NAME <= ?

=head2 C<like>

Like tag

    {like NAME}   ->   NAME like ?

=head2 C<in>

In tag.

    {in NAME COUNT}   ->   NAME in [?, ?, ..]

=head2 C<insert_param>

Insert parameter tag.

    {insert_param NAME1 NAME2}   ->   (NAME1, NAME2) values (?, ?)

=head2 C<update_param>

Updata parameter tag.

    {update_param NAME1 NAME2}   ->   set NAME1 = ?, NAME2 = ?

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


