package DBIx::Custom;

our $VERSION = '0.1629';

use 5.008001;
use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder;
use DBIx::Custom::Model;
use Encode qw/encode_utf8 decode_utf8/;

__PACKAGE__->attr([qw/data_source dbh
                      dbi_options password user/]);

__PACKAGE__->attr(cache => 1);
__PACKAGE__->attr(filters => sub {
    {
        encode_utf8 => sub { encode_utf8($_[0]) },
        decode_utf8 => sub { decode_utf8($_[0]) }
    }
});
__PACKAGE__->attr(filter_check => 1);
__PACKAGE__->attr(query_builder  => sub {DBIx::Custom::QueryBuilder->new});
__PACKAGE__->attr(result_class => 'DBIx::Custom::Result');
__PACKAGE__->attr(table_class => 'DBIx::Custom::Table');

# DBI methods
foreach my $method (qw/begin_work commit rollback/) {
    my $code = sub {
        my $self = shift;
        my $ret = eval {$self->dbh->$method};
        croak $@ if $@;
        return $ret;
    };
    no strict 'refs';
    my $pkg = __PACKAGE__;
    *{"${pkg}::$method"} = $code;
};

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;

    # Method
    my ($package, $method) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

    # Helper
    $self->{_helpers} ||= {};
    croak qq/Can't locate object method "$method" via "$package"/
      unless my $helper = $self->{_helpers}->{$method};

    # Run
    return $self->$helper(@_);
}

sub apply_filter {
    my $self = shift;
    
    $self->{filter} ||= {};
    
    # Table
    my $table = shift;
    
    if (@_) {
        # Column infomations
        my @cs = @_;
        
        # Initialize filters
        $self->{filter}{out} ||= {};
        $self->{filter}{in} ||= {};
        
        # Create auto filters
        foreach my $c (@cs) {
            croak "Usage \$dbi->apply_filter(" .
                  "TABLE, COLUMN, {in => INFILTER, out => OUTFILTER}, ...)"
              unless ref $c eq 'ARRAY' && @$c == 3;
            
            # Column
            my $column = $c->[0];
            
            # Bind filter
            my $out_filter  = $c->[1];
            if (ref $out_filter eq 'CODE') {
                $self->{filter}{out}{$table}{$column}
                  = $out_filter;
                $self->{filter}{out}{$table}{"$table.$column"}
                  = $out_filter;
            }
            else {
                croak qq{"$out_filter" is not registered}
                  unless exists $self->filters->{$out_filter};
                
                $self->{filter}{out}{$table}{$column}
                  = $self->filters->{$out_filter};
                $self->{filter}{out}{$table}{"$table.$column"}
                  = $self->filters->{$out_filter};
              }
            
            # Fetch filter
            my $in_filter = $c->[2];
            if (ref $in_filter eq 'CODE') {
                $self->{filter}{in}{$table}{$column}
                  = $in_filter;
                $self->{filter}{in}{$table}{"$table.$column"}
                  = $in_filter;
            }
            else {
                croak qq{"$in_filter" is not registered}
                  unless exists $self->filters->{$in_filter};
                $self->{filter}{in}{$table}{$column}
                  = $self->filters->{$in_filter};
                $self->{filter}{in}{$table}{"$table.$column"}
                  = $self->filters->{$in_filter};
            }
        }
        
        return $self;
    }
    
    return $self->{filter};
}

sub helper {
    my $self = shift;
    
    # Merge
    my $helpers = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_helpers} = {%{$self->{_helpers} || {}}, %$helpers};
    
    return $self;
}

sub connect {
    my $proto = shift;
    
    my $self;
    # Create
    if (my $class = ref $proto) {
        my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        $self = $proto;
        
        foreach my $attr (keys %$args) {
            $self->{$attr} = $args->{$attr};
        }
        
        # Check attribute names
        my @attrs = keys %$self;
        foreach my $attr (@attrs) {
            croak qq{"$attr" is invalid attribute name}
              unless $self->can($attr);
        }
    }
    else {
        $self = $proto->SUPER::new(@_);
    }
    
    # Information
    my $data_source = $self->data_source;
    
    croak qq{"data_source" must be specified to connect method"}
      unless $data_source;
    
    my $user        = $self->user;
    my $password    = $self->password;
    my $dbi_options = $self->dbi_options || {};
    
    # Connect
    my $dbh = eval {DBI->connect(
        $data_source,
        $user,
        $password,
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            %$dbi_options
        }
    )};
    
    # Connect error
    croak $@ if $@;
    
    # Database handle
    $self->dbh($dbh);
    
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
        $query = DBIx::Custom::Query->new($q) if $q;
    }
    
    unless ($query) {

        # Create SQL object
        my $builder = $self->query_builder;
        
        # Create query
        $query = $builder->build_query($source);

        # Cache query
        $self->cache_method->($self, $source,
                             {sql     => $query->sql, 
                              columns => $query->columns})
          if $cache;
    }
    
    # Prepare statement handle
    my $sth;
    eval { $sth = $self->dbh->prepare($query->{sql})};
    $self->_croak($@, qq{. SQL: "$query->{sql}"}) if $@;
    
    # Set statement handle
    $query->sth($sth);
    
    return $query;
}

our %VALID_DELETE_ARGS
  = map { $_ => 1 } qw/table where append filter allow_delete_all/;

sub delete {
    my ($self, %args) = @_;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is invalid argument}
          unless $VALID_DELETE_ARGS{$name};
    }
    
    # Arguments
    my $table            = $args{table} || '';
    my $where            = $args{where} || {};
    my $append = $args{append};
    my $filter           = $args{filter};
    my $allow_delete_all = $args{allow_delete_all};

    # Where keys
    my @where_keys = keys %$where;
    
    # Not exists where keys
    croak qq{"where" argument must be specified and } .
          qq{contains the pairs of column name and value}
      if !@where_keys && !$allow_delete_all;
    
    # Where clause
    my $where_clause = '';
    if (@where_keys) {
        $where_clause = 'where ';
        $where_clause .= "{= $_} and " for @where_keys;
        $where_clause =~ s/ and $//;
    }
    
    # Source of SQL
    my $source = "delete from $table $where_clause";
    $source .= " $append" if $append;
    
    # Execute query
    my $ret_val = $self->execute(
        $source, param  => $where, filter => $filter,
        table => $table);
    
    return $ret_val;
}

sub delete_all { shift->delete(allow_delete_all => 1, @_) }

sub DESTROY { }

our %VALID_EXECUTE_ARGS = map { $_ => 1 } qw/param filter table/;

sub execute{
    my ($self, $query, %args)  = @_;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is invalid argument}
          unless $VALID_EXECUTE_ARGS{$name};
    }
    
    my $params = $args{param} || {};
    
    # First argument is the soruce of SQL
    $query = $self->create_query($query)
      unless ref $query;
    
    # Auto filter
    my $filter = {};
    my $tables = $args{table} || [];
    $tables = [$tables]
      unless ref $tables eq 'ARRAY';
    foreach my $table (@$tables) {
        $filter = {
            %$filter,
            %{$self->{filter}{out}->{$table} || {}}
        }
    }
    
    # Filter argument
    my $f = $args{filter} || $query->filter || {};
    foreach my $column (keys %$f) {
        my $fname = $f->{$column};
        if (!defined $fname) {
            $f->{$column} = undef;
        }
        elsif (ref $fname ne 'CODE') {
          croak qq{"$fname" is not registered"}
            unless exists $self->filters->{$fname};
          
          $f->{$column} = $self->filters->{$fname};
        }
    }
    $filter = {%$filter, %$f};
    
    # Create bind values
    my $binds = $self->_build_binds($params, $query->columns, $filter);
    
    # Execute
    my $sth      = $query->sth;
    my $affected;
    eval {$affected = $sth->execute(@$binds)};
    $self->_croak($@) if $@;
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        
        # Auto in filter
        my $in_filter = {};
        foreach my $table (@$tables) {
            $in_filter = {
                %$in_filter,
                %{$self->{filter}{in}{$table} || {}}
            }
        }
        
        # Result
        my $result = $self->result_class->new(
            sth            => $sth,
            filters        => $self->filters,
            filter_check   => $self->filter_check,
            default_filter => $self->{default_in_filter},
            filter         => $in_filter || {}
        );

        return $result;
    }
    return $affected;
}

sub expand {
    my $self = shift;
    my $source = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    my $table = (keys %$source)[0];
    my $param = $source->{$table};
    
    # Expand table name
    my $expand = {};
    foreach my $column (keys %$param) {
        $expand->{"$table.$column"} = $param->{$column};
    }
    
    return %$expand;
}

our %VALID_INSERT_ARGS = map { $_ => 1 } qw/table param append
                                            filter/;
sub insert {
    my ($self, %args) = @_;

    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is invalid argument}
          unless $VALID_INSERT_ARGS{$name};
    }
    
    # Arguments
    my $table  = $args{table} || '';
    my $param  = $args{param} || {};
    my $append = $args{append} || '';
    my $filter = $args{filter};
    
    # Insert keys
    my @insert_keys = keys %$param;
    
    # Templte for insert
    my $source = "insert into $table {insert_param "
               . join(' ', @insert_keys) . '}';
    $source .= " $append" if $append;
    
    # Execute query
    my $ret_val = $self->execute(
        $source,
        param  => $param,
        filter => $filter,
        table => $table
    );
    
    return $ret_val;
}

sub iterate_all_columns {
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
            $cb->($table, $column, $column_info);
        }
    }
}

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attribute names
    my @attrs = keys %$self;
    foreach my $attr (@attrs) {
        croak qq{"$attr" is invalid attribute name}
          unless $self->can($attr);
    }
    
    return $self;
}

sub register_filter {
    my $invocant = shift;
    
    # Register filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

our %VALID_SELECT_ARGS
  = map { $_ => 1 } qw/table column where append relation filter/;

sub select {
    my ($self, %args) = @_;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is invalid argument}
          unless $VALID_SELECT_ARGS{$name};
    }
    
    # Arguments
    my $tables = $args{table} || [];
    $tables = [$tables] unless ref $tables eq 'ARRAY';
    my $columns  = $args{column} || [];
    my $where    = $args{where};
    my $relation = $args{relation};
    my $append   = $args{append};
    my $filter   = $args{filter};
    
    # Source of SQL
    my $source = 'select ';
    
    # Column clause
    if (@$columns) {
        foreach my $column (@$columns) {
            $source .= "$column, ";
        }
        $source =~ s/, $/ /;
    }
    else {
        $source .= '* ';
    }
    
    # Table
    $source .= 'from ';
    foreach my $table (@$tables) {
        $source .= "$table, ";
    }
    $source =~ s/, $/ /;
    
    # Where clause
    my $param;
    if (ref $where eq 'HASH' && keys %$where) {
        $param = $where;
        $source .= 'where (';
        foreach my $where_key (keys %$where) {
            $source .= "{= $where_key} and ";
        }
        $source =~ s/ and $//;
        $source .= ') ';
    }
    elsif (ref $where eq 'ARRAY') {
        my$where_str = $where->[0] || '';
        $param = $where->[1];
        
        $source .= "where ($where_str) ";
    }
    
    # Relation
    if ($relation) {
        $source .= $where ? "and " : "where ";
        foreach my $rkey (keys %$relation) {
            $source .= "$rkey = " . $relation->{$rkey} . " and ";
        }
    }
    $source =~ s/ and $//;
    
    # Append some statement
    $source .= " $append" if $append;
    
    # Execute query
    my $result = $self->execute(
        $source, param  => $param, filter => $filter,
        table => $tables);    
    
    return $result;
}

sub table {
    my $self = shift;
    my $name = shift;
    
    # Table class
    my $table_class = $self->table_class;
    croak qq{Invalid table class name "$table_class"}
      unless $table_class =~ /^[\w:]+$/;
    unless ($table_class->can('isa')) {
        eval "require $table_class";
        croak $@ if $@;
    }
    # Create table
    $self->{_tables} ||= {};
    $self->{_tables}->{$name}
        = $table_class->new(name => $name, dbi => $self)
      unless defined $self->{_tables}->{$name};
    
    # Helper
    $self->{_tables}->{$name}->helper(@_) if @_;
    
    return $self->{_tables}{$name};
}

sub txn_scope {
    my $self = shift;
    
    require DBIx::TransactionManager;
    
    $self->{_transaction_manager}
      ||= DBIx::TransactionManager->new($self->dbh);
    
    return $self->{_transaction_manager}->txn_scope;
}

our %VALID_UPDATE_ARGS
  = map { $_ => 1 } qw/table param
                       where append filter allow_update_all/;

sub update {
    my ($self, %args) = @_;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is invalid argument}
          unless $VALID_UPDATE_ARGS{$name};
    }
    
    # Arguments
    my $table            = $args{table} || '';
    my $param            = $args{param} || {};
    my $where            = $args{where} || {};
    my $append = $args{append} || '';
    my $filter           = $args{filter};
    my $allow_update_all = $args{allow_update_all};
    
    # Update keys
    my @update_keys = keys %$param;
    
    # Where keys
    my @where_keys = keys %$where;
    
    # Not exists where keys
    croak qq{"where" argument must be specified and } .
          qq{contains the pairs of column name and value}
      if !@where_keys && !$allow_update_all;
    
    # Update clause
    my $update_clause = '{update_param ' . join(' ', @update_keys) . '}';
    
    # Where clause
    my $where_clause = '';
    my $new_where = {};
    
    if (@where_keys) {
        $where_clause = 'where ';
        $where_clause .= "{= $_} and " for @where_keys;
        $where_clause =~ s/ and $//;
    }
    
    # Source of SQL
    my $source = "update $table $update_clause $where_clause";
    $source .= " $append" if $append;
    
    # Rearrange parameters
    foreach my $wkey (@where_keys) {
        
        if (exists $param->{$wkey}) {
            $param->{$wkey} = [$param->{$wkey}]
              unless ref $param->{$wkey} eq 'ARRAY';
            
            push @{$param->{$wkey}}, $where->{$wkey};
        }
        else {
            $param->{$wkey} = $where->{$wkey};
        }
    }
    
    # Execute query
    my $ret_val = $self->execute($source, param  => $param, 
                                 filter => $filter,
                                 table => $table);
    
    return $ret_val;
}

sub update_all { shift->update(allow_update_all => 1, @_) };

sub _build_binds {
    my ($self, $params, $columns, $filter) = @_;
    
    # bind values
    my @binds;
    
    # Build bind values
    my $count = {};
    foreach my $column (@$columns) {
        
        # Value
        my $value = ref $params->{$column} eq 'ARRAY'
                  ? $params->{$column}->[$count->{$column} || 0]
                  : $params->{$column};
        
        # Filter
        my $f = $filter->{$column} || $self->{default_out_filter} || '';
        
        push @binds, $f ? $f->($value) : $value;
        
        # Count up 
        $count->{$column}++;
    }
    
    return \@binds;
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

# Deprecated
__PACKAGE__->attr(cache_method => sub {
    sub {
        my $self = shift;
        
        $self->{_cached} ||= {};
        
        if (@_ > 1) {
            $self->{_cached}{$_[0]} = $_[1] 
        }
        else {
            return $self->{_cached}{$_[0]}
        }
    }
});

sub default_bind_filter {
    my $self = shift;
    
    if (@_) {
        my $fname = $_[0];
        
        if (@_ && !$fname) {
            $self->{default_out_filter} = undef;
        }
        else {
            croak qq{"$fname" is not registered}
              unless exists $self->filters->{$fname};
        
            $self->{default_out_filter} = $self->filters->{$fname};
        }
        return $self;
    }
    
    return $self->{default_out_filter};
}

sub default_fetch_filter {
    my $self = shift;
    my $fname = $_[0];
    
    if (@_) {
        if (@_ && !$fname) {
            $self->{default_in_filter} = undef;
        }
        else {
            croak qq{"$fname" is not registered}
              unless exists $self->filters->{$fname};
        
            $self->{default_in_filter} = $self->filters->{$fname};
        }
        
        return $self;
    }
    
    return $self->{default_in_filter}
}

1;

=head1 NAME

DBIx::Custom - DBI interface, having hash parameter binding and filtering system

=head1 SYNOPSYS

Connect to the database.
    
    use DBIx::Custom;
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=dbname",
                                    user => 'ken', password => '!LFKD%$&');

Insert, update, and delete

    # Insert 
    $dbi->insert(table  => 'book',
                 param  => {title => 'Perl', author => 'Ken'},
                 filter => {title => 'encode_utf8'});
    
    # Update 
    $dbi->update(table  => 'book', 
                 param  => {title => 'Perl', author => 'Ken'}, 
                 where  => {id => 5},
                 filter => {title => 'encode_utf8'});
    
    # Update all
    $dbi->update_all(table  => 'book',
                     param  => {title => 'Perl'},
                     filter => {title => 'encode_utf8'});
    
    # Delete
    $dbi->delete(table  => 'book',
                 where  => {author => 'Ken'},
                 filter => {title => 'encode_utf8'});
    
    # Delete all
    $dbi->delete_all(table => 'book');

Select

    # Select
    my $result = $dbi->select(table => 'book');
    
    # Select, more complex
    my $result = $dbi->select(
        table  => 'book',
        column => [qw/author title/],
        where  => {author => 'Ken'},
        append => 'order by id limit 5',
        filter => {title => 'encode_utf8'}
    );
    
    # Select, join table
    my $result = $dbi->select(
        table    => ['book', 'rental'],
        column   => ['book.name as book_name']
        relation => {'book.id' => 'rental.book_id'}
    );
    
    # Select, more flexible where
    my $result = $dbi->select(
        table  => 'book',
        where  => ['{= author} and {like title}', 
                   {author => 'Ken', title => '%Perl%'}]
    );

Execute SQL

    # Execute SQL
    $dbi->execute("select title from book");
    
    # Execute SQL with hash binding and filtering
    $dbi->execute("select id from book where {= author} and {like title}",
                  param  => {author => 'ken', title => '%Perl%'},
                  filter => {title => 'encode_utf8'});

    # Create query and execute it
    my $query = $dbi->create_query(
        "select id from book where {= author} and {like title}"
    );
    $dbi->execute($query, param => {author => 'Ken', title => '%Perl%'})

Other features.

    # Get DBI object
    my $dbh = $dbi->dbh;

Fetch row.

    # Fetch
    while (my $row = $result->fetch) {
        # ...
    }
    
    # Fetch hash
    while (my $row = $result->fetch_hash) {
        
    }
    
=head1 DESCRIPTIONS

L<DBIx::Custom> is one of L<DBI> interface modules,
such as L<DBIx::Class>, L<DBIx::Simple>.

This module is not O/R mapper. O/R mapper is useful,
but you must learn many syntax of the O/R mapper,
which is almost another language.
Created SQL statement is offten not effcient and damage SQL performance.
so you have to execute raw SQL in the end.

L<DBIx::Custom> is middle area between L<DBI> and O/R mapper.
L<DBIx::Custom> provide flexible hash parameter binding and filtering system,
and suger methods, such as C<insert()>, C<update()>, C<delete()>, C<select()>
to execute SQL easily.

L<DBIx::Custom> respects SQL. SQL is very complex and not beautiful,
but de-facto standard,
so all people learing database know it.
If you already know SQL,
you learn a little thing to use L<DBIx::Custom>.

See L<DBIx::Custom::Guides> for more details.

=head1 ATTRIBUTES

=head2 C<cache>

    my $cache = $dbi->cache;
    $dbi      = $dbi->cache(1);

Enable parsed L<DBIx::Custom::Query> object caching.
Default to 1.

=head2 C<data_source>

    my $data_source = $dbi->data_source;
    $dbi            = $dbi->data_source("DBI:mysql:database=dbname");

Data source.
C<connect()> method use this value to connect the database.

=head2 C<dbh>

    my $dbh = $dbi->dbh;
    $dbi    = $dbi->dbh($dbh);

L<DBI> object. You can call all methods of L<DBI>.

=head2 C<dbi_options>

    my $dbi_options = $dbi->dbi_options;
    $dbi            = $dbi->dbi_options($dbi_options);

DBI options.
C<connect()> method use this value to connect the database.

Default filter when row is fetched.

=head2 C<filters>

    my $filters = $dbi->filters;
    $dbi        = $dbi->filters(\%filters);

Filter functions.
"encode_utf8" and "decode_utf8" is registered by default.

=head2 C<filter_check>

    my $filter_check = $dbi->filter_check;
    $dbi             = $dbi->filter_check(0);

B<this attribute is now deprecated and has no mean
because check is always done>. 

=head2 C<password>

    my $password = $dbi->password;
    $dbi         = $dbi->password('lkj&le`@s');

Password.
C<connect()> method use this value to connect the database.

=head2 C<query_builder>

    my $sql_class = $dbi->query_builder;
    $dbi          = $dbi->query_builder(DBIx::Custom::QueryBuilder->new);

SQL builder. C<query_builder()> must be 
the instance of L<DBIx::Custom::QueryBuilder> subclass.
Default to L<DBIx::Custom::QueryBuilder> object.

=head2 C<result_class>

    my $result_class = $dbi->result_class;
    $dbi             = $dbi->result_class('DBIx::Custom::Result');

Result class for select statement.
Default to L<DBIx::Custom::Result>.

=head2 C<user>

    my $user = $dbi->user;
    $dbi     = $dbi->user('Ken');

User name.
C<connect()> method use this value to connect the database.
    
=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<(experimental) apply_filter >

    $dbi->apply_filter(
        $table,
        $column1 => {in => $infilter1, out => $outfilter1}
        $column2 => {in => $infilter2, out => $outfilter2}
        ...,
    );

C<apply_filter> is automatically filter for columns of table.
This have effect C<insert>, C<update>, C<delete>. C<select>
and L<DBIx::Custom::Result> object. but this has'nt C<execute> method.

If you want to have effect C<execute()> method, use C<table>
arguments.

    $result = $dbi->execute(
        "select * from table1 where {= key1} and {= key2};",
         param => {key1 => 1, key2 => 2},
         table => ['table1']
    );
    
=head2 C<begin_work>

    $dbi->begin_work;

Start transaction.
This is same as L<DBI>'s C<begin_work>.

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<commit>

    $dbi->commit;

Commit transaction.
This is same as L<DBI>'s C<commit>.

=head2 C<connect>

    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=dbname",
                                    user => 'ken', password => '!LFKD%$&');

Create a new L<DBIx::Custom> object and connect to the database.
L<DBIx::Custom> is a wrapper of L<DBI>.
C<AutoCommit> and C<RaiseError> options are true, 
and C<PrintError> option is false by default. 

=head2 C<create_query>
    
    my $query = $dbi->create_query(
        "select * from book where {= author} and {like title};"
    );

Create the instance of L<DBIx::Custom::Query> from the source of SQL.
If you want to get high performance,
use C<create_query()> method and execute it by C<execute()> method
instead of suger methods.

    $dbi->execute($query, {author => 'Ken', title => '%Perl%'});

=head2 C<execute>

    my $result = $dbi->execute($query,  param => $params, filter => \%filter);
    my $result = $dbi->execute($source, param => $params, filter => \%filter);

Execute query or the source of SQL.
Query is L<DBIx::Custom::Query> object.
Return value is L<DBIx::Custom::Result> if select statement is executed,
or the count of affected rows if insert, update, delete statement is executed.

B<Example:>

    my $result = $dbi->execute(
        "select * from book where {= author} and {like title}", 
        param => {author => 'Ken', title => '%Perl%'}
    );
    
    while (my $row = $result->fetch) {
        my $author = $row->[0];
        my $title  = $row->[1];
    }

=head2 C<(experimental) expand>

    my %expand = $dbi->expand($source);

The following hash

    {book => {title => 'Perl', author => 'Ken'}}

is expanded to

    ('book.title' => 'Perl', 'book.author' => 'Ken')

This is used in C<select()>


    
=head2 C<delete>

    $dbi->delete(table  => $table,
                 where  => \%where,
                 append => $append,
                 filter => \%filter);

Execute delete statement.
C<delete> method have C<table>, C<where>, C<append>, and C<filter> arguments.
C<table> is a table name.
C<where> is where clause. this must be hash reference.
C<append> is a string added at the end of the SQL statement.
C<filter> is filters when parameter binding is executed.
Return value of C<delete()> is the count of affected rows.

B<Example:>

    $dbi->delete(table  => 'book',
                 where  => {id => 5},
                 append => 'some statement',
                 filter => {id => 'encode_utf8'});

=head2 C<delete_all>

    $dbi->delete_all(table => $table);

Execute delete statement to delete all rows.
Arguments is same as C<delete> method,
except that C<delete_all> don't have C<where> argument.
Return value of C<delete_all()> is the count of affected rows.

B<Example:>
    
    $dbi->delete_all(table => 'book');

=head2 C<(experimental) helper>

    $dbi->helper(
        update_or_insert => sub {
            my $self = shift;
            # do something
        },
        find_or_create   => sub {
            my $self = shift;
            # do something
        }
    );

Register helper methods. These method is called from L<DBIx::Custom> object directory.

    $dbi->update_or_insert;
    $dbi->find_or_create;

=head2 C<insert>

    $dbi->insert(table  => $table, 
                 param  => \%param,
                 append => $append,
                 filter => \%filter);

Execute insert statement.
C<insert> method have C<table>, C<param>, C<append>
and C<filter> arguments.
C<table> is a table name.
C<param> is the pairs of column name value. this must be hash reference.
C<append> is a string added at the end of the SQL statement.
C<filter> is filters when parameter binding is executed.
This is overwrites C<default_bind_filter>.
Return value of C<insert()> is the count of affected rows.

B<Example:>

    $dbi->insert(table  => 'book', 
                 param  => {title => 'Perl', author => 'Taro'},
                 append => "some statement",
                 filter => {title => 'encode_utf8'})

=head2 C<new>

    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=dbname",
                                    user => 'ken', password => '!LFKD%$&');

Create a new L<DBIx::Custom> object.

=head2 C<(experimental) iterate_all_columns>

    $dbi->iterate_all_columns(
        sub {
            my ($table, $column, $column_info) = @_;
            
            # do something;
        }
    );

Iterate all columns of all tables. Argument is callback.
You can do anything by callback.

=head2 C<register_filter>

    $dbi->register_filter(%filters);
    $dbi->register_filter(\%filters);
    
Register filter. Registered filters is available in the following attributes
or arguments.

=over 4

=item *

C<filter> argument of C<insert()>, C<update()>,
C<update_all()>, C<delete()>, C<delete_all()>, C<select()>
methods

=item *

C<execute()> method

=item *

C<default_filter> and C<filter> of C<DBIx::Custom::Query>

=item *

C<default_filter> and C<filter> of C<DBIx::Custom::Result>

=back

B<Example:>

    $dbi->register_filter(
        encode_utf8 => sub {
            my $value = shift;
            
            require Encode;
            
            return Encode::encode('UTF-8', $value);
        },
        decode_utf8 => sub {
            my $value = shift;
            
            require Encode;
            
            return Encode::decode('UTF-8', $value)
        }
    );

=head2 C<rollback>

    $dbi->rollback;

Rollback transaction.
This is same as L<DBI>'s C<rollback>.

=head2 C<select>
    
    my $result = $dbi->select(table    => $table,
                              column   => [@column],
                              where    => \%where,
                              append   => $append,
                              relation => \%relation,
                              filter   => \%filter);

Execute select statement.
C<select> method have C<table>, C<column>, C<where>, C<append>,
C<relation> and C<filter> arguments.
C<table> is a table name.
C<where> is where clause. this is normally hash reference.
C<append> is a string added at the end of the SQL statement.
C<filter> is filters when parameter binding is executed.

B<Example:>

    # select * from book;
    my $result = $dbi->select(table => 'book');
    
    # select * from book where title = ?;
    my $result = $dbi->select(table => 'book', where => {title => 'Perl'});
    
    # select title, author from book where id = ? for update;
    my $result = $dbi->select(
        table  => 'book',
        column => ['title', 'author'],
        where  => {id => 1},
        appned => 'for update'
    );
    
    # select book.name as book_name from book, rental
    # where book.id = rental.book_id;
    my $result = $dbi->select(
        table    => ['book', 'rental'],
        column   => ['book.name as book_name']
        relation => {'book.id' => 'rental.book_id'}
    );

If you use more complex condition,
you can specify a array reference to C<where> argument.

    my $result = $dbi->select(
        table  => 'book',
        column => ['title', 'author'],
        where  => ['{= title} or {like author}',
                   {title => '%Perl%', author => 'Ken'}]
    );

First element is a string. it contains tags,
such as "{= title} or {like author}".
Second element is paramters.

=head2 C<update>

    $dbi->update(table  => $table, 
                 param  => \%params,
                 where  => \%where,
                 append => $append,
                 filter => \%filter)

Execute update statement.
C<update> method have C<table>, C<param>, C<where>, C<append>
and C<filter> arguments.
C<table> is a table name.
C<param> is column-value pairs. this must be hash reference.
C<where> is where clause. this must be hash reference.
C<append> is a string added at the end of the SQL statement.
C<filter> is filters when parameter binding is executed.
This is overwrites C<default_bind_filter>.
Return value of C<update()> is the count of affected rows.

B<Example:>

    $dbi->update(table  => 'book',
                 param  => {title => 'Perl', author => 'Taro'},
                 where  => {id => 5},
                 append => "some statement",
                 filter => {title => 'encode_utf8'});

=head2 C<(experimental) txn_scope>

    {
        my $txn = $dbi->txn_scope;
        $dbi->insert(table => 'book', param => {title => 'Perl'});
        $dbi->insert(table => 'book', param => {title => 'Good days'});
        $txn->commit;
    }

Create transaction scope. If you escape scope(that is { .. }) and commited,
Rollback is automatically done.

Note that this is feature of L<DBIx::TransactionManager>
L<DBIx::TransactionManager> is required.

=head2 C<(experimental) table>

    $dbi->table('book',
        insert => sub { ... },
        update => sub { ... }
    );
    
    my $table = $dbi->table('book');

Create a L<DBIx::Custom::Table> object,
or get a L<DBIx::Custom::Table> object.

=head2 C<update_all>

    $dbi->update_all(table  => $table, 
                     param  => \%params,
                     filter => \%filter,
                     append => $append);

Execute update statement to update all rows.
Arguments is same as C<update> method,
except that C<update_all> don't have C<where> argument.
Return value of C<update_all()> is the count of affected rows.

B<Example:>

    $dbi->update_all(table  => 'book', 
                     param  => {author => 'taro'},
                     filter => {author => 'encode_utf8'});

=head2 C<(deprecated) default_bind_filter>

    my $default_bind_filter = $dbi->default_bind_filter;
    $dbi                    = $dbi->default_bind_filter($fname);

Default filter when parameter binding is executed.

=head2 C<(deprecated) default_fetch_filter>

    my $default_fetch_filter = $dbi->default_fetch_filter;
    $dbi = $dbi->default_fetch_filter($fname);

=head2 C<(deprecated) cache_method>

    $dbi          = $dbi->cache_method(\&cache_method);
    $cache_method = $dbi->cache_method

Method to set and get caches.

B<Example:>

    $dbi->cache_method(
        sub {
            my $self = shift;
            
            $self->{_cached} ||= {};
            
            if (@_ > 1) {
                $self->{_cached}{$_[0]} = $_[1] 
            }
            else {
                return $self->{_cached}{$_[0]}
            }
        }
    );

=head1 STABILITY

L<DBIx::Custom> is now stable. APIs keep backword compatible in the feature.

=head1 BUGS

Please tell me bugs if found.

C<< <kimoto.yuki at gmail.com> >>

L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut


