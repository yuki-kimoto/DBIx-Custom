package DBIx::Custom;

our $VERSION = '0.1617';

use 5.008001;
use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder;
use Encode qw/encode_utf8 decode_utf8/;

__PACKAGE__->attr('dbh');
__PACKAGE__->attr([qw/user password data_source/]);
__PACKAGE__->attr([qw/default_bind_filter default_fetch_filter/]);

__PACKAGE__->dual_attr('filters', default => sub { {} },
                                  inherit => 'hash_copy');
__PACKAGE__->register_filter(
    encode_utf8 => sub { encode_utf8($_[0]) },
    decode_utf8 => sub { decode_utf8($_[0]) }
);

__PACKAGE__->attr(result_class => 'DBIx::Custom::Result');
__PACKAGE__->attr(query_builder  => sub {DBIx::Custom::QueryBuilder->new});

__PACKAGE__->attr(cache => 1);
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

__PACKAGE__->attr(filter_check => 1);

sub connect {
    my $proto = shift;
    
    # Create
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Information
    my $data_source = $self->data_source;
    my $user        = $self->user;
    my $password    = $self->password;
    
    
    # Connect
    my $dbh = eval {DBI->connect(
        $data_source,
        $user,
        $password,
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
        }
    )};
    
    # Connect error
    croak $@ if $@;
    
    # Database handle
    $self->dbh($dbh);
    
    return $self;
}

sub register_filter {
    my $invocant = shift;
    
    # Register filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

our %VALID_INSERT_ARGS = map { $_ => 1 } qw/table param append filter/;

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
    my $ret_val = $self->execute($source, param  => $param, 
                                          filter => $filter);
    
    return $ret_val;
}

our %VALID_UPDATE_ARGS
  = map { $_ => 1 } qw/table param where append filter allow_update_all/;

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
                                 filter => $filter);
    
    return $ret_val;
}

sub update_all { shift->update(allow_update_all => 1, @_) };

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
    my $ret_val = $self->execute($source, param  => $where, 
                                 filter => $filter);
    
    return $ret_val;
}

sub delete_all { shift->delete(allow_delete_all => 1, @_) }

our %VALID_SELECT_ARGS
  = map { $_ => 1 } qw/table column where append relation filter param/;

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
    if (ref $where eq 'HASH') {
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
    my $result = $self->execute($source, param  => $param, 
                                         filter => $filter);
    
    return $result;
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

our %VALID_EXECUTE_ARGS = map { $_ => 1 } qw/param filter/;

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
    
    my $filter = $args{filter} || $query->filter || {};
    
    # Create bind value
    my $bind_values = $self->_build_bind_values($query, $params, $filter);
    
    # Execute
    my $sth      = $query->sth;
    my $affected;
    eval {$affected = $sth->execute(@$bind_values)};
    $self->_croak($@) if $@;
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        
        # Create result
        my $result = $self->result_class->new(
            sth            => $sth,
            default_filter => $self->default_fetch_filter,
            filters        => $self->filters,
            filter_check   => $self->filter_check
        );

        return $result;
    }
    return $affected;
}

sub _build_bind_values {
    my ($self, $query, $params, $filter) = @_;
    
    # binding values
    my @bind_values;

    # Filter
    $filter ||= {};
    
    # Parameter
    $params ||= {};
    
    # Check filter
    $self->_check_filter($self->filters, $filter,
                         $self->default_bind_filter, $params)
      if $self->filter_check;
    
    # Build bind values
    my $count = {};
    foreach my $column (@{$query->columns}) {
        
        # Value
        my $value = ref $params->{$column} eq 'ARRAY'
                  ? $params->{$column}->[$count->{$column} || 0]
                  : $params->{$column};
        
        # Filtering
        my $fname = $filter->{$column} || $self->default_bind_filter || '';
        my $filter_func = $fname ? $self->filters->{$fname} : undef;
        push @bind_values, $filter_func
                         ? $filter_func->($value)
                         : $value;
        
        # Count up 
        $count->{$column}++;
    }
    
    return \@bind_values;
}

sub _check_filter {
    my ($self, $filters, $filter, $default_filter, $params) = @_;
    
    # Filter name not exists
    foreach my $fname (values %$filter) {
        croak qq{Bind filter "$fname" is not registered}
          unless exists $filters->{$fname};
    }
    
    # Default filter name not exists
    croak qq{Default bind filter "$default_filter" is not registered}
      if $default_filter && ! exists $filters->{$default_filter};
    
    # Column name not exists
    foreach my $column (keys %$filter) {
        
        croak qq{Column name "$column" in bind filter is not found in paramters}
          unless exists $params->{$column};
    }
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
    $dbi->insert(table  => 'books',
                 param  => {title => 'Perl', author => 'Ken'},
                 filter => {title => 'encode_utf8'});
    
    # Update 
    $dbi->update(table  => 'books', 
                 param  => {title => 'Perl', author => 'Ken'}, 
                 where  => {id => 5},
                 filter => {title => 'encode_utf8'});
    
    # Update all
    $dbi->update_all(table  => 'books',
                     param  => {title => 'Perl'},
                     filter => {title => 'encode_utf8'});
    
    # Delete
    $dbi->delete(table  => 'books',
                 where  => {author => 'Ken'},
                 filter => {title => 'encode_utf8'});
    
    # Delete all
    $dbi->delete_all(table => 'books');

Select

    # Select
    my $result = $dbi->select(table => 'books');
    
    # Select, more complex
    my $result = $dbi->select(
        table  => 'books',
        column => [qw/author title/],
        where  => {author => 'Ken'},
        append => 'order by id limit 5',
        filter => {title => 'encode_utf8'}
    );
    
    # Select, join table
    my $result = $dbi->select(
        table    => ['books', 'rental'],
        column   => ['books.name as book_name']
        relation => {'books.id' => 'rental.book_id'}
    );
    
    # Select, more flexible where
    my $result = $dbi->select(
        table  => 'books',
        where  => ['{= author} and {like title}', 
                   {author => 'Ken', title => '%Perl%'}]
    );

Execute SQL

    # Execute SQL
    $dbi->execute("select title from books");
    
    # Execute SQL with hash binding and filtering
    $dbi->execute("select id from books where {= author} and {like title}",
                  param  => {author => 'ken', title => '%Perl%'},
                  filter => {title => 'encode_utf8'});

    # Create query and execute it
    my $query = $dbi->create_query(
        "select id from books where {= author} and {like title}"
    );
    $dbi->execute($query, param => {author => 'Ken', title => '%Perl%'})

Other features.

    # Default filter
    $dbi->default_bind_filter('encode_utf8');
    $dbi->default_fetch_filter('decode_utf8');

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

=head2 1. Features

L<DBIx::Custom> is one of L<DBI> interface modules,
such as L<DBIx::Class>, L<DBIx::Simple>.

This module is not O/R mapper. O/R mapper is useful,
but you must learn many syntax of the O/R mapper,
which is almost another language.
Created SQL statement is offten not effcient and damage SQL performance.
so you have to execute raw SQL in the end.

L<DBIx::Custom> is middle area between L<DBI> and O/R mapper.
L<DBIx::Custom> provide flexible hash parameter binding and filtering system,
and suger methods, such as C<select()>, C<update()>, C<delete()>, C<select()>
to execute SQL easily.

L<DBIx::Custom> respects SQL. SQL is very complex and not beautiful,
but de-facto standard,
so all people learing database know it.
If you already know SQL,
you learn a little thing to use L<DBIx::Custom>.

=head2 2. Connect to the database

C<connect()> method create a new L<DBIx::Custom>
object and connect to the database.

    use DBIx::Custom;
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=dbname",
                                    user => 'ken', password => '!LFKD%$&');

If database is SQLite, use L<DBIx::Custom::SQLite> instead.
you connect database easily.

    use DBIx::Custom::SQLite;
    my $dbi = DBIx::Custom::SQLite->connect(database => 'dbname');
    
If database is  MySQL, use L<DBIx::Custom::MySQL>.

    use DBIx::Custom::MySQL;
    my $dbi = DBIx::Custom::MySQL->connect(
        database => 'dbname',
        user     => 'ken',
        password => '!LFKD%$&'
    );

=head2 3. Suger methods

L<DBIx::Custom> has suger methods, such as C<insert()>, C<update()>,
C<delete()> or C<select()>. If you want to do small works,
You don't have to create SQL statements.

=head3 insert()

Execute insert statement.

    $dbi->insert(table  => 'books',
                 param  => {title => 'Perl', author => 'Ken'});

The following SQL is executed.

    insert into (title, author) values (?, ?);

The values of C<title> and C<author> is embedded into the placeholders.

C<append> and C<filter> argument can be specified.
See also "METHODS" section.

=head3 update()

Execute update statement.

    $dbi->update(table  => 'books', 
                 param  => {title => 'Perl', author => 'Ken'}, 
                 where  => {id => 5});

The following SQL is executed.

    update books set title = ?, author = ?;

The values of C<title> and C<author> is embedded into the placeholders.

C<append> and C<filter> argument can be specified.
See also "METHOD" section.

If you want to update all rows, use C<update_all()> method.

=head3 delete()

Execute delete statement.

    $dbi->delete(table  => 'books',
                 where  => {author => 'Ken'});

The following SQL is executed.

    delete from books where id = ?;

The value of C<id> is embedded into the placehodler.

C<append> and C<filter> argument can be specified.
see also "METHODS" section.

If you want to delete all rows, use C<delete_all()> method.

=head3 select()

Execute select statement, only C<table> argument specified :

    my $result = $dbi->select(table => 'books');

The following SQL is executed.

    select * from books;

the result of C<select()> method is L<DBIx::Custom::Result> object.
You can fetch a row by C<fetch()> method.

    while (my $row = $result->fetch) {
        my $title  = $row->[0];
        my $author = $row->[1];
    }

L<DBIx::Custom::Result> has various methods to fetch row.
See "4. Fetch row".

C<column> and C<where> arguments specified.

    my $result = $dbi->select(
        table  => 'books',
        column => [qw/author title/],
        where  => {author => 'Ken'}
    );

The following SQL is executed.

    select author, title from books where author = ?;

the value of C<author> is embdded into the placeholder.

If you want to join tables, specify C<relation> argument. 

    my $result = $dbi->select(
        table    => ['books', 'rental'],
        column   => ['books.name as book_name']
        relation => {'books.id' => 'rental.book_id'}
    );

The following SQL is executed.

    select books.name as book_name from books, rental
    where books.id = rental.book_id;

If you want to add some string to the end of SQL statement,
use C<append> argument.

    my $result = $dbi->select(
        table  => 'books',
        where  => {author => 'Ken'},
        append => 'order by price limit 5',
    );

The following SQL is executed.

    select * books where author = ? order by price limit 5;

C<filter> argument can be specified.
see also "METHODS" section.

=head2 4. Fetch row

C<select()> method return L<DBIx::Custom::Result> object.
You can fetch row by various methods.
Note that in this section, array means array reference,
and hash meanse hash reference.

Fetch row into array.
    
    while (my $row = $result->fetch) {
        my $author = $row->[0];
        my $title  = $row->[1];
        
    }

Fetch only a first row into array.

    my $row = $result->fetch_first;

Fetch multiple rows into array of array.

    while (my $rows = $result->fetch_multi(5)) {
        my $first_author  = $rows->[0][0];
        my $first_title   = $rows->[0][1];
        my $second_author = $rows->[1][0];
        my $second_value  = $rows->[1][1];
    
    }
    
Fetch all rows into array of array.

    my $rows = $result->fetch_all;

Fetch row into hash.

    # Fetch a row into hash
    while (my $row = $result->fetch_hash) {
        my $title  = $row->{title};
        my $author = $row->{author};
        
    }

Fetch only a first row into hash

    my $row = $result->fetch_hash_first;
    
Fetch multiple rows into array of hash

    while (my $rows = $result->fetch_hash_multi(5)) {
        my $first_title   = $rows->[0]{title};
        my $first_author  = $rows->[0]{author};
        my $second_title  = $rows->[1]{title};
        my $second_author = $rows->[1]{author};
    
    }
    
Fetch all rows into array of hash

    my $rows = $result->fetch_hash_all;

If you want to access statement handle of L<DBI>, use C<sth> attribute.

    my $sth = $result->sth;

=head2 5. Hash parameter binding

L<DBIx::Custom> provides hash parameter binding.

At frist, I show normal parameter binding.

    use DBI;
    my $dbh = DBI->connect(...);
    my $sth = $dbh->prepare(
        "select * from books where author = ? and title like ?;"
    );
    $sth->execute('Ken', '%Perl%');

This is very good way because database system can enable SQL caching,
and parameter is quoted automatically. this is secure.

L<DBIx::Custom> hash parameter binding system improve
normal parameter binding to use hash parameter.

    my $result = $dbi->execute(
        "select * from books where {= author} and {like title};"
        param => {author => 'Ken', title => '%Perl%'}
    );

This is same as the normal way, execpt that the parameter is hash.
{= author} and {like title} is called C<tag>.
tag is expand to placeholder string internally.

    select * from books where {= author} and {like title}
      -> select * from books where author = ? and title like ?;

The following tags is available.

    [TAG]                       [REPLACED]
    {? NAME}               ->   ?
    {= NAME}               ->   NAME = ?
    {<> NAME}              ->   NAME <> ?
    
    {< NAME}               ->   NAME < ?
    {> NAME}               ->   NAME > ?
    {>= NAME}              ->   NAME >= ?
    {<= NAME}              ->   NAME <= ?
    
    {like NAME}            ->   NAME like ?
    {in NAME COUNT}        ->   NAME in [?, ?, ..]
    
    {insert_param NAME1 NAME2}   ->   (NAME1, NAME2) values (?, ?)
    {update_param NAME1 NAME2}   ->   set NAME1 = ?, NAME2 = ?

See also L<DBIx::Custom::QueryBuilder>.

C<{> and C<}> is reserved. If you use these charactors,
you must escape them using '\'. Note that '\' is
already perl escaped charactor, so you must write '\\'. 

    'select * from books \\{ something statement \\}'

=head2 6. Filtering

Usually, Perl string is kept as internal string.
If you want to save the string to database, You must encode the string.
Filtering system help you to convert a data to another data
when you save to the data and get the data form database.

If you want to register filter, use C<register_filter()> method.

    $dbi->register_filter(
        to_upper_case => sub {
            my $value = shift;
            return uc $value;
        }
    );

C<encode_utf8> and C<decode_utf8> filter is registerd by default.

You can specify these filters to C<filter> argument of C<execute()> method.

    my $result = $dbi->execute(
        "select * from books where {= author} and {like title};"
        param  => {author => 'Ken', title => '%Perl%'},
        filter => {author => 'to_upper_case, title => 'encode_utf8'}
    );

C<filter> argument can be specified to suger methods, such as
C<insert()>, C<update()>, C<update_all()>,
C<delete()>, C<delete_all()>, C<select()>.

    # insert(), having filter argument
    $dbi->insert(table  => 'books',
                 param  => {title => 'Perl', author => 'Ken'},
                 filter => {title => 'encode_utf8'});
    
    # select(), having filter argument
    my $result = $dbi->select(
        table  => 'books',
        column => [qw/author title/],
        where  => {author => 'Ken'},
        append => 'order by id limit 1',
        filter => {title => 'encode_utf8'}
    );

Filter works each parmeter, but you prepare default filter for all parameters.

    $dbi->default_bind_filter('encode_utf8');

C<filter()> argument overwrites this default filter.
    
    $dbi->default_bind_filter('encode_utf8');
    $dbi->insert(
        table  => 'books',
        param  => {title => 'Perl', author => 'Ken', price => 1000},
        filter => {author => 'to_upper_case', price => undef}
    );

This is same as the following example.

    $dbi->insert(
        table  => 'books',
        param  => {title => 'Perl', author => 'Ken', price => 1000},
        filter => {title => 'encode_uft8' author => 'to_upper_case'}
    );

You can also specify filter when the row is fetched. This is reverse of bind filter.

    my $result = $dbi->select(table => 'books');
    $result->filter({title => 'decode_utf8', author => 'to_upper_case'});

Filter works each column value, but you prepare a default filter
for all clumn value.

    $dbi->default_fetch_filter('decode_utf8');

C<filter()> method of L<DBIx::Custom::Result>
overwrites this default filter.

    $dbi->default_fetch_filter('decode_utf8');
    my $result = $dbi->select(
        table => 'books',
        columns => ['title', 'author', 'price']
    );
    $result->filter({author => 'to_upper_case', price => undef});

This is same as the following one.

    my $result = $dbi->select(
        table => 'books',
        columns => ['title', 'author', 'price']
    );
    $result->filter({title => 'decode_utf8', author => 'to_upper_case'});

Note that in fetch filter, column names must be lower case
even if the column name conatains upper case charactors.
This is requirment not to depend database systems.

=head2 7. Get high performance

=head3 Disable filter checking

Filter checking is executed by default.
This is done to check right filter name is specified,
but sometimes damage performance.

If you disable this filter checking,
Set C<filter_check> attribute to 0.

    $dbi->filter_check(0);

=head3 Use execute() method instead suger methods

If you execute insert statement by C<insert()> method,
you sometimes can't get required performance.

C<insert()> method is a little slow because SQL statement and statement handle
is created every time.

In that case, you can prepare a query by C<create_query()> method.
    
    my $query = $dbi->create_query(
        "insert into books {insert_param title author};"
    );

Return value of C<create_query()> is L<DBIx::Custom::Query> object.
This keep the information of SQL and column names.

    {
        sql     => 'insert into books (title, author) values (?, ?);',
        columns => ['title', 'author']
    }

Execute query repeatedly.
    
    my $inputs = [
        {title => 'Perl',      author => 'Ken'},
        {title => 'Good days', author => 'Mike'}
    ];
    
    foreach my $input (@$inputs) {
        $dbi->execute($query, $input);
    }

This is faster than C<insert()> method.

=head3 caching

C<execute()> method caches the parsed result of the source of SQL.
Default to 1

    $dbi->cache(1);

Caching is on memory, but you can change this by C<cache_method()>.
First argument is L<DBIx::Custom> object.
Second argument is a source of SQL,
such as "select * from books where {= title} and {= author};";
Third argument is parsed result, such as
{sql => "select * from books where title = ? and author = ?",
 columns => ['title', 'author']}, this is hash reference.
If arguments is more than two, this method is called to set cache.
If not, this method is called to get cache.

    $dbi->cache_method(sub {
        sub {
            my $self = shift;
            
            $self->{_cached} ||= {};
            
            # Set cache
            if (@_ > 1) {
                $self->{_cached}{$_[0]} = $_[1] 
            }
            
            # Get cache
            else {
                return $self->{_cached}{$_[0]}
            }
        }
    });

=head2 8. More features

=head3 Get DBI object

You can get L<DBI> object and call any method of L<DBI>.

    $dbi->dbh->begin_work;
    $dbi->dbh->commit;
    $dbi->dbh->rollback;

=head3 Change Result class

You can change Result class if you need.

    package Your::Result;
    use base 'DBIx::Custom::Result';
    
    sub some_method { ... }

    1;
    
    package main;
    
    use Your::Result;
    
    my $dbi = DBIx::Custom->connect(...);
    $dbi->result_class('Your::Result');

=head3 Custamize SQL builder object

You can custamize SQL builder object

    my $dbi = DBIx::Custom->connect(...);
    $dbi->query_builder->register_tag_processor(
        name => sub {
           ...
        }
    );

=head1 ATTRIBUTES

=head2 C<user>

    my $user = $dbi->user;
    $dbi     = $dbi->user('Ken');

User name.
C<connect()> method use this value to connect the database.
    
=head2 C<password>

    my $password = $dbi->password;
    $dbi         = $dbi->password('lkj&le`@s');

Password.
C<connect()> method use this value to connect the database.

=head2 C<data_source>

    my $data_source = $dbi->data_source;
    $dbi            = $dbi->data_source("DBI:mysql:database=dbname");

Data source.
C<connect()> method use this value to connect the database.

=head2 C<dbh>

    my $dbh = $dbi->dbh;
    $dbi    = $dbi->dbh($dbh);

L<DBI> object. You can call all methods of L<DBI>.

=head2 C<filters>

    my $filters = $dbi->filters;
    $dbi        = $dbi->filters(\%filters);

Filter functions.
"encode_utf8" and "decode_utf8" is registered by default.

=head2 C<default_bind_filter>

    my $default_bind_filter = $dbi->default_bind_filter
    $dbi                    = $dbi->default_bind_filter('encode_utf8');

Default filter when parameter binding is executed.

=head2 C<default_fetch_filter>

    my $default_fetch_filter = $dbi->default_fetch_filter;
    $dbi                     = $dbi->default_fetch_filter('decode_utf8');

Default filter when row is fetched.

=head2 C<result_class>

    my $result_class = $dbi->result_class;
    $dbi             = $dbi->result_class('DBIx::Custom::Result');

Result class for select statement.
Default to L<DBIx::Custom::Result>.

=head2 C<query_builder>

    my $sql_class = $dbi->query_builder;
    $dbi          = $dbi->query_builder(DBIx::Custom::QueryBuilder->new);

SQL builder. C<query_builder()> must be 
the instance of L<DBIx::Custom::QueryBuilder> subclass.
Default to L<DBIx::Custom::QueryBuilder> object.

=head2 C<cache>

    my $cache = $dbi->cache;
    $dbi      = $dbi->cache(1);

Enable parsed L<DBIx::Custom::Query> object caching.
Default to 1.

=head2 C<cache_method>

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

=head2 C<filter_check>

    my $filter_check = $dbi->filter_check;
    $dbi             = $dbi->filter_check(0);

Enable filter check. 
Default to 1.
This check maybe damege performance.
If you require performance, set C<filter_check> attribute to 0.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<connect>

    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=dbname",
                                    user => 'ken', password => '!LFKD%$&');

Create a new L<DBIx::Custom> object and connect to the database.
L<DBIx::Custom> is a wrapper of L<DBI>.
C<AutoCommit> and C<RaiseError> options are true, 
and C<PrintError> option is false by default. 

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

    $dbi->insert(table  => 'books', 
                 param  => {title => 'Perl', author => 'Taro'},
                 append => "some statement",
                 filter => {title => 'encode_utf8'})

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

    $dbi->update(table  => 'books',
                 param  => {title => 'Perl', author => 'Taro'},
                 where  => {id => 5},
                 append => "some statement",
                 filter => {title => 'encode_utf8'});

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

    $dbi->update_all(table  => 'books', 
                     param  => {author => 'taro'},
                     filter => {author => 'encode_utf8'});

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

    $dbi->delete(table  => 'books',
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
    
    $dbi->delete_all(table => 'books');

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

    # select * from books;
    my $result = $dbi->select(table => 'books');
    
    # select * from books where title = ?;
    my $result = $dbi->select(table => 'books', where => {title => 'Perl'});
    
    # select title, author from books where id = ? for update;
    my $result = $dbi->select(
        table  => 'books',
        column => ['title', 'author'],
        where  => {id => 1},
        appned => 'for update'
    );
    
    # select books.name as book_name from books, rental
    # where books.id = rental.book_id;
    my $result = $dbi->select(
        table    => ['books', 'rental'],
        column   => ['books.name as book_name']
        relation => {'books.id' => 'rental.book_id'}
    );

If you use more complex condition,
you can specify a array reference to C<where> argument.

    my $result = $dbi->select(
        table  => 'books',
        column => ['title', 'author'],
        where  => ['{= title} or {like author}',
                   {title => '%Perl%', author => 'Ken'}]
    );

First element is a string. it contains tags,
such as "{= title} or {like author}".
Second element is paramters.

=head2 C<create_query>
    
    my $query = $dbi->create_query(
        "select * from books where {= author} and {like title};"
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
        "select * from books where {= author} and {like title}", 
        param => {author => 'Ken', title => '%Perl%'}
    );
    
    while (my $row = $result->fetch) {
        my $author = $row->[0];
        my $title  = $row->[1];
    }

=head2 C<register_filter>

    $dbi->register_filter(%filters);
    $dbi->register_filter(\%filters);
    
Register filter. Registered filters is available in the following attributes
or arguments.

=over 4

=item *

C<default_bind_filter>, C<default_fetch_filter>

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


