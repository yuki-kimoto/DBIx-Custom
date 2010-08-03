package DBIx::Custom;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::QueryBuilder;
use DBIx::Custom::Query;
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
    
    # Add filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

our %VALID_INSERT_ARGS = map { $_ => 1 } qw/table param append filter/;

sub insert {
    my ($self, %args) = @_;

    # Check arguments
    foreach my $name (keys %args) {
        croak "\"$name\" is invalid name"
          unless $VALID_INSERT_ARGS{$name};
    }
    
    # Arguments
    my $table  = $args{table} || '';
    my $param  = $args{param} || {};
    my $append = $args{append} || '';
    my $filter = $args{filter};
    
    # Insert keys
    my @insert_keys = keys %$param;
    
    # Not exists insert keys
    croak("Key-value pairs for insert must be specified to 'insert' second argument")
      unless @insert_keys;
    
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
        croak "\"$name\" is invalid name"
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
    
    # Not exists update kyes
    croak("Key-value pairs for update must be specified to 'update' second argument")
      unless @update_keys;
    
    # Where keys
    my @where_keys = keys %$where;
    
    # Not exists where keys
    croak("Key-value pairs for where clause must be specified to 'update' third argument")
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
    
    # Rearrange parammeters
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
        croak "\"$name\" is invalid name"
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
    croak("Key-value pairs for where clause must be specified to 'delete' second argument")
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
        croak "\"$name\" is invalid name"
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

sub build_query {
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
        croak($@) if $@;
        
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
        croak "\"$name\" is invalid name"
          unless $VALID_EXECUTE_ARGS{$name};
    }
    
    my $params = $args{param} || {};
    
    # First argument is SQL template
    unless (ref $query eq 'DBIx::Custom::Query') {
        my $source;
        
        if (ref $query eq 'ARRAY') {
            $source = $query->[0];
        }
        else { $source = $query }
        
        $query = $self->build_query($source);
    }
    
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
        
        croak "\"$column\" is not exists in params"
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
                croak "Not exists filter \"$fname\""
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

DBIx::Custom - DBI with hash parameter binding and filtering system

=cut

our $VERSION = '0.1604';

=head1 STABILITY

This module is not stable. Method name and implementations will be changed.

=head1 SYNOPSYS
    
    # Connect
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');
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
    
    # Select
    my $result = $dbi->select(table => 'books');
    
    # Select(more complex)
    my $result = $dbi->select(
        table  => 'books',
        column => [qw/author title/],
        where  => {author => 'Ken'},
        append => 'order by id limit 1',
        filter => {tilte => 'encode_utf8'}
    );
    
    # Select(Join table)
    my $result = $dbi->select(
        table => ['books', 'rental'],
        column => ['books.name as book_name']
        relation => {'books.id' => 'rental.book_id'}
    );
    
    # Execute SQL
    $dbi->execute("select title from books");
    
    # Execute SQL with parameters and filter
    $dbi->execute("select id from books where {= author} && {like title}",
                  param  => {author => 'ken', title => '%Perl%'},
                  filter => {tilte => 'encode_utf8'});

    # Create query and execute it
    my $query = $dbi->build_query(
        "select id from books where {= author} && {like title}"
    );
    $dbi->execute($query, param => {author => 'ken', title => '%Perl%'})
    
    # Default filter
    $dbi->default_bind_filter('encode_utf8');
    $dbi->default_fetch_filter('decode_utf8');
    
    # Fetch
    while (my $row = $result->fetch) {
        # ...
    }
    
    # Fetch hash
    while (my $row = $result->fetch_hash) {
        
    }
    
    # Get DBI object
    my $dbh = $dbi->dbh;

=head1 DESCRIPTION

L<DBIx::Custom> is useful L<DBI> extention.
This module have hash parameter binding and filtering system.

Normally, binding parameter is array.
L<DBIx::Custom> enable you to pass binding parameter as hash.

This module also provide filtering system.
You can filter the binding parameter
or the value of fetching row.

And have useful method such as insert(), update(), delete(), and select().

=head2 Features

=over 4

=item *

Hash parameter binding.

=item *

Value filtering.

=item *

Provide suger methods, such as insert(), update(), delete(), and select().

=back

=head1 ATTRIBUTES

=head2 C<user>

    my $user = $dbi->user;
    $dbi     = $dbi->user('Ken');

Database user name.
This is used for connect().
    
=head2 C<password>

    my $password = $dbi->password;
    $dbi         = $dbi->password('lkj&le`@s');

Database password.
This is used for connect().

=head2 C<data_source>

    my $data_source = $dbi->data_source;
    $dbi            = $dbi->data_source("dbi:mysql:dbname=$database");

Database data source.
This is used for connect().

=head2 C<dbh>

    my $dbh = $dbi->dbh;
    $dbi    = $dbi->dbh($dbh);

Database handle. This is a L<DBI> object.
You can call all methods of L<DBI>

    my $sth    = $dbi->dbh->prepare("...");
    my $errstr = $dbi->dbh->errstr;
    $dbi->dbh->begin_work;
    $dbi->dbh->commit;
    $dbi->dbh->rollback;
    
=head2 C<filters>

    my $filters = $dbi->filters;
    $dbi        = $dbi->filters({%filters});

Filter functions.
By default, "encode_utf8" and "decode_utf8" is registered.

    $encode_utf8 = $dbi->filters->{encode_utf8};
    $decode_utf8 = $dbi->filters->{decode_utf8};

=head2 C<default_bind_filter>

    my $default_bind_filter = $dbi->default_bind_filter
    $dbi                    = $dbi->default_bind_filter('encode_utf8');

Default filter for value binding

=head2 C<default_fetch_filter>

    my $default_fetch_filter = $dbi->default_fetch_filter;
    $dbi                     = $dbi->default_fetch_filter('decode_utf8');

Default filter for fetching.

=head2 C<result_class>

    my $result_class = $dbi->result_class;
    $dbi             = $dbi->result_class('DBIx::Custom::Result');

Result class for select statement.
Default to L<DBIx::Custom::Result>.

=head2 C<sql_builder>

    my $sql_class = $dbi->sql_builder;
    $dbi          = $dbi->sql_builder(DBIx::Custom::QueryBuilder->new);

SQL builder. sql_builder must be 
the instance of L<DBIx::Custom::QueryBuilder> subclass
Default to DBIx::Custom::QueryBuilder.

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 C<connect>

    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');

Connect to database.
"AutoCommit" and "RaiseError" option is true, 
and "PrintError" option is false by default.

=head2 C<insert>

    $affected = $dbi->insert(table  => $table, 
                             param  => {%param},
                             append => $append,
                             filter => {%filter});

Insert row.
Retrun value is the count of affected rows.
    
B<Example:>

    $dbi->insert(table  => 'books', 
                 param  => {title => 'Perl', author => 'Taro'},
                 append => "some statement",
                 filter => {title => 'encode_utf8'})

=head2 C<update>

    $affected = $dbi->update(table  => $table, 
                             param  => {%params},
                             where  => {%where},
                             append => $append,
                             filter => {%filter})

Update rows.
Retrun value is the count of affected rows.

B<Example:>

    $dbi->update(table  => 'books',
                 param  => {title => 'Perl', author => 'Taro'},
                 where  => {id => 5},
                 append => "some statement",
                 filter => {title => 'encode_utf8'});

=head2 C<update_all>

    $affected = $dbi->update_all(table  => $table, 
                                 param  => {%params},
                                 filter => {%filter},
                                 append => $append);

Update all rows.
Retrun value is the count of affected rows.

B<Example:>

    $dbi->update_all(table  => 'books', 
                     param  => {author => 'taro'},
                     filter => {author => 'encode_utf8'});

=head2 C<delete>

    $affected = $dbi->delete(table  => $table,
                             where  => {%where},
                             append => $append,
                             filter => {%filter});

Delete rows.
Retrun value is the count of affected rows.
    
B<Example:>

    $dbi->delete(table  => 'books',
                 where  => {id => 5},
                 append => 'some statement',
                 filter => {id => 'encode_utf8'});

=head2 C<delete_all>

    $affected = $dbi->delete_all(table => $table);

Delete all rows.
Retrun value is the count of affected rows.

B<Example:>
    
    $dbi->delete_all(table => 'books');

=head2 C<select>
    
    $result = $dbi->select(table    => $table,
                           column   => [@column],
                           where    => {%where},
                           append   => $append,
                           relation => {%relation},
                           filter   => {%filter});

Select rows.
Return value is the instance of L<DBIx::Custom::Result>.

B<Example:>

    # select * from books;
    $result = $dbi->select(table => 'books');
    
    # select * from books where title = 'Perl';
    $result = $dbi->select(table => 'books', where => {title => 1});
    
    # select title, author from books where id = 1 for update;
    $result = $dbi->select(
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

=head2 C<build_query>
    
    my $query = $dbi->build_query(
        "select * from authors where {= name} and {= age};"
    );

Build the instance of L<DBIx::Custom::Query>
using L<DBIx::Custom::QueryBuilder>.

=head2 C<execute>

    $result = $dbi->execute($query,    param => $params, filter => {%filter});
    $result = $dbi->execute($source, param => $params, filter => {%filter});

Execute the instace of L<DBIx::Custom::Query> or
the string written by SQL template.
Return value is the instance of L<DBIx::Custom::Result>.

B<Example:>

    $result = $dbi->execute("select * from authors where {= name} and {= age}", 
                            param => {name => 'taro', age => 19});
    
    while (my $row = $result->fetch) {
        # do something
    }

=head2 C<register_filter>

    $dbi->register_filter(%filters);
    $dbi->register_filter(\%filters);
    
Resister filter.

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

=head2 C<cache>

    $dbi   = $dbi->cache(1);
    $cache = $dbi->cache;

Cache the result of parsing SQL template.
Default to 1.

=head2 C<cache_method>

    $dbi          = $dbi->cache_method(\&cache_method);
    $cache_method = $dbi->cache_method

Method for cache.

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

=head1 BUGS

Please tell me bugs.

C<< <kimoto.yuki at gmail.com> >>

L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut


