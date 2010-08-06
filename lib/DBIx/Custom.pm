package DBIx::Custom;

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
__PACKAGE__->attr(sql_builder  => sub {DBIx::Custom::QueryBuilder->new});

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
        croak qq{"$name" is invalid name}
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
    my $source = "insert into $table {insert "
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
        croak qq{"$name" is invalid name}
          unless $VALID_UPDATE_ARGS{$name};
    }
    
    # Arguments
    my $table            = $args{table} || '';
    my $param            = $args{param} || {};
    my $where            = $args{where} || {};
    my $append_statement = $args{append} || '';
    my $filter           = $args{filter};
    my $allow_update_all = $args{allow_update_all};
    
    # Update keys
    my @update_keys = keys %$param;
    
    # Where keys
    my @where_keys = keys %$where;
    
    # Not exists where keys
    croak qq{"where" must contain column-value pair}
      if !@where_keys && !$allow_update_all;
    
    # Update clause
    my $update_clause = '{update ' . join(' ', @update_keys) . '}';
    
    # Where clause
    my $where_clause = '';
    my $new_where = {};
    
    if (@where_keys) {
        $where_clause = 'where ';
        foreach my $where_key (@where_keys) {
            
            $where_clause .= "{= $where_key} and ";
        }
        $where_clause =~ s/ and $//;
    }
    
    # Template for update
    my $source = "update $table $update_clause $where_clause";
    $source .= " $append_statement" if $append_statement;
    
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
        croak qq{"$name" is invalid name}
          unless $VALID_DELETE_ARGS{$name};
    }
    
    # Arguments
    my $table            = $args{table} || '';
    my $where            = $args{where} || {};
    my $append_statement = $args{append};
    my $filter           = $args{filter};
    my $allow_delete_all = $args{allow_delete_all};
    
    # Where keys
    my @where_keys = keys %$where;
    
    # Not exists where keys
    croak qq{Key-value pairs for where clause must be specified to "delete" second argument}
      if !@where_keys && !$allow_delete_all;
    
    # Where clause
    my $where_clause = '';
    if (@where_keys) {
        $where_clause = 'where ';
        foreach my $wkey (@where_keys) {
            $where_clause .= "{= $wkey} and ";
        }
        $where_clause =~ s/ and $//;
    }
    
    # Template for delete
    my $source = "delete from $table $where_clause";
    $source .= " $append_statement" if $append_statement;
    
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
        croak qq{"$name" is invalid name}
          unless $VALID_SELECT_ARGS{$name};
    }
    
    # Arguments
    my $tables = $args{table} || [];
    $tables = [$tables] unless ref $tables eq 'ARRAY';
    my $columns  = $args{column} || [];
    my $where    = $args{where} || {};
    my $relation = $args{relation};
    my $append   = $args{append};
    my $filter   = $args{filter};
    my $param    = $args{param} || {};
    
    # SQL template for select statement
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
    my @where_keys = keys %$where;
    if (@where_keys) {
        $source .= 'where ';
        foreach my $where_key (@where_keys) {
            $source .= "{= $where_key} and ";
        }
    }
    $source =~ s/ and $//;
    
    # Relation
    if ($relation) {
        $source .= @where_keys ? "and " : "where ";
        foreach my $rkey (keys %$relation) {
            $source .= "$rkey = " . $relation->{$rkey} . " and ";
        }
    }
    $source =~ s/ and $//;
    
    # Append some statement
    $source .= " $append" if $append;
    
    # Execute query
    my $result = $self->execute($source, param  => $where, 
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
        my $builder = $self->sql_builder;
        
        # Create query
        $query = eval{$builder->build_query($source)};
        croak $@ if $@;
        
        # Cache query
        $self->cache_method->($self, $source,
                             {sql     => $query->sql, 
                              columns => $query->columns})
          if $cache;
    }
    
    # Prepare statement handle
    my $sth = eval {$self->dbh->prepare($query->{sql})};
    croak $@ if $@;
    
    # Set statement handle
    $query->sth($sth);
    
    return $query;
}

our %VALID_EXECUTE_ARGS = map { $_ => 1 } qw/param filter/;

sub execute{
    my ($self, $query, %args)  = @_;
    
    # Check arguments
    foreach my $name (keys %args) {
        croak qq{"$name" is invalid name}
          unless $VALID_EXECUTE_ARGS{$name};
    }
    
    my $params = $args{param} || {};
    
    # First argument is SQL template
    $query = $self->create_query($query)
      unless ref $query;
    
    my $filter = $args{filter} || $query->filter || {};
    
    # Create bind value
    my $bind_values = $self->_build_bind_values($query, $params, $filter);
    
    # Execute
    my $sth      = $query->sth;
    my $affected = eval {$sth->execute(@$bind_values)};
    croak $@ if $@;
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        
        # Create result
        my $result = $self->result_class->new(
            sth            => $sth,
            default_filter => $self->default_fetch_filter,
            filters        => $self->filters
        );

        return $result;
    }
    return $affected;
}

sub _build_bind_values {
    my ($self, $query, $params, $filter) = @_;
    
    # binding values
    my @bind_values;
    
    # Build bind values
    my $count = {};
    foreach my $column (@{$query->columns}) {
        
        croak qq{"$column" is not exists in params}
          unless exists $params->{$column};
        
        # Value
        my $value = ref $params->{$column} eq 'ARRAY'
                  ? $params->{$column}->[$count->{$column} || 0]
                  : $params->{$column};
        
        # Filter
        $filter ||= {};
        
        # Filter name
        my $fname = $filter->{$column} || $self->default_bind_filter || '';
        
        my $filter_func;
        if ($fname) {
            
            if (ref $fname eq 'CODE') {
                $filter_func = $fname;
            }
            else {
                my $filters = $self->filters;
                croak qq{Not exists filter "$fname"}
                  unless exists $filters->{$fname};
                $filter_func = $filters->{$fname};
            }            
        }
        
        push @bind_values, $filter_func
                         ? $filter_func->($value)
                         : $value;
        
        # Count up 
        $count->{$column}++;
    }
    
    return \@bind_values;
}

=head1 NAME

DBIx::Custom - DBI interface, having hash parameter binding and filtering system

=cut

our $VERSION = '0.1606';

=head1 STABILITY

B<This module is not stable>. 
Method name and implementations will be changed for a while.

=head1 SYNOPSYS

Connect to the database.
    
    use DBIx::Custom;
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');

Insert, update, and delete

    # Insert 
    $dbi->insert(table  => 'books',
                 param  => {title => 'perl', author => 'Ken'},
                 filter => {title => 'encode_utf8'});
    
    # Update 
    $dbi->update(table  => 'books', 
                 param  => {title => 'aaa', author => 'Ken'}, 
                 where  => {id => 5},
                 filter => {title => 'encode_utf8'});
    
    # Update all
    $dbi->update_all(table  => 'books',
                     param  => {title => 'aaa'},
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
        append => 'order by id limit 1',
        filter => {title => 'encode_utf8'}
    );
    
    # Select, join table
    my $result = $dbi->select(
        table    => ['books', 'rental'],
        column   => ['books.name as book_name']
        relation => {'books.id' => 'rental.book_id'}
    );

Execute SQL

    # Execute SQL
    $dbi->execute("select title from books");
    
    # Execute SQL with hash binding and filtering
    $dbi->execute("select id from books where {= author} && {like title}",
                  param  => {author => 'ken', title => '%Perl%'},
                  filter => {title => 'encode_utf8'});

    # Create query and execute it
    my $query = $dbi->create_query(
        "select id from books where {= author} && {like title}"
    );
    $dbi->execute($query, param => {author => 'ken', title => '%Perl%'})

More features.

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
    

=head1 DESCRIPTION

=head2 1. Features

L<DBIx::Custom> is one of L<DBI> interface modules,
such as L<DBIx::Class>, L<DBIx::Simple>.

This module is not O/R mapper. O/R mapper is useful,
but you must learn many syntax of the O/R mapper,
which is almost another language
Create SQL statement is offten not effcient and damage SQL performance.
so you have to execute raw SQL in the end.

L<DBIx::Custom> is middle area between L<DBI> and O/R mapper.
L<DBIx::Custom> provide flexible hash parameter binding adn filtering system,
and suger method, such as C<select()>, C<update()>, C<delete()>, C<select()>
to execute a query easily.

L<DBIx::Custom> respects SQL. SQL is not beautiful, but de-facto standard,
so all people learing database system know it.
If you know SQL statement,
you learn a little thing about L<DBIx::Custom> to do your works.

=head2 2. Basic usage

=head3 connect to the database

C<connect()> method create a new L<DBIx::Custom>
object and connect to the database.

    use DBIx::Custom;
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');

=head3 Suger methods

L<DBIx::Custom> has suger methods, such as C<insert()>, C<update()>,
C<delete()> and C<select()>. If you want to do simple works,
You don't have to create SQL statement.

B<Execute insert statement:>





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

=head2 C<sql_builder>

    my $sql_class = $dbi->sql_builder;
    $dbi          = $dbi->sql_builder(DBIx::Custom::QueryBuilder->new);

SQL builder. sql_builder must be 
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

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<connect>

    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=dbname",
                                    user => 'ken', password => '!LFKD%$&');

Create a new L<DBIx::Custom> object and connect to the database.
L<DBIx::Custom> is a wrapper of L<DBI>.
C<AutoCommit> and C<RaiseError> option is true, 
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
C<param> is column-value pairs. this must be hash reference.
C<append> is a string added at the end of the SQL statement.
C<filter> is filters when parameter binding is executed.
This is overwrites C<default_bind_filter>.
Return value of C<insert> is the count of affected rows.

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
Return value of C<update> is the count of affected rows.

B<Example:>

    $dbi->update(table  => 'books',
                 param  => {title => 'Perl', author => 'Taro'},
                 where  => {id => 5},
                 append => "for update",
                 filter => {title => 'encode_utf8'});

=head2 C<update_all>

    $dbi->update_all(table  => $table, 
                     param  => \%params,
                     filter => \%filter,
                     append => $append);

Execute update statement to update all rows.
Arguments is same as C<update> method,
except that C<update_all> don't have C<where> argument.
Return value of C<update_all> is the count of affected rows.

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
Return value of C<delete> is the count of affected rows.

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
Return value of C<delete_all> is the count of affected rows.

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
C<select> method have C<table>, C<column>, C<where>, C<append>
C<relation> and C<filter> arguments.
C<table> is a table name.
C<where> is where clause. this must be hash reference
or a string containing such tags as "{= title} or {= author}".
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

=head2 C<create_query>
    
    my $query = $dbi->create_query(
        "select * from authors where {= name} and {= age};"
    );

Create the instance of L<DBIx::Custom::Query> from SQL source.

=head2 C<execute>

    my $result = $dbi->execute($query,  param => $params, filter => \%filter);
    my $result = $dbi->execute($source, param => $params, filter => \%filter);

Execute query or SQL source. Query is L<DBIx::Csutom::Query> object.
Return value is L<DBIx::Custom::Result> in select statement,
or the count of affected rows in insert, update, delete statement.

B<Example:>

    my $result = $dbi->execute("select * from authors where {= name} and {= age}", 
                            param => {name => 'taro', age => 19});
    
    while (my $row = $result->fetch) {
        # do something
    }

=head2 C<register_filter>

    $dbi->register_filter(%filters);
    $dbi->register_filter(\%filters);
    
Register filter. Registered filters is available in the following methods
or arguments.

=over 4

=item *

C<default_bind_filter()>

=item *

C<default_fetch_filter()>

=item *

C<filter> argument of C<insert()>, C<update()>,
C<update_all()>, C<delete()>, C<delete_all()>, C<select()>,
C<execute> method.

=item *

C<DBIx::Custom::Query::default_filter()>

=item *

C<DBIx::Csutom::Query::filter()>

=item *

C<DBIx::Custom::Result::default_filter()>

=item *

C<DBIx::Custom::Result::filter()>

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


