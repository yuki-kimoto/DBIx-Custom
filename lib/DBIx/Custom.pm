package DBIx::Custom;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::SQLTemplate;
use DBIx::Custom::Query;

__PACKAGE__->attr('dbh');

__PACKAGE__->class_attr(_query_caches     => sub { {} });
__PACKAGE__->class_attr(_query_cache_keys => sub { [] });

__PACKAGE__->class_attr('query_cache_max', default => 50,
                                           inherit => 'scalar_copy');

__PACKAGE__->attr([qw/user password data_source/]);
__PACKAGE__->attr([qw/database host port/]);
__PACKAGE__->attr([qw/default_query_filter default_fetch_filter options/]);

__PACKAGE__->dual_attr('filters', default => sub { {} },
                                  inherit => 'hash_copy');
__PACKAGE__->register_filter(
    encode_utf8 => sub { encode('UTF-8', $_[0]) },
    decode_utf8 => sub { decode('UTF-8', $_[0]) }
);

__PACKAGE__->attr(result_class => 'DBIx::Custom::Result');
__PACKAGE__->attr(sql_template => sub { DBIx::Custom::SQLTemplate->new });



sub register_filter {
    my $invocant = shift;
    
    # Add filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

sub auto_commit {
    my $self = shift;
    
    # Not connected
    croak("Not yet connect to database") unless $self->connected;
    
    if (@_) {
        
        # Set AutoCommit
        $self->dbh->{AutoCommit} = $_[0];
        
        return $self;
    }
    return $self->dbh->{AutoCommit};
}

sub connect {
    my $proto = shift;
    
    # Create
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Information
    my $data_source = $self->data_source;
    my $user        = $self->user;
    my $password    = $self->password;
    my $options     = $self->options;
    
    # Connect
    my $dbh = eval{DBI->connect(
        $data_source,
        $user,
        $password,
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            %{$options || {} }
        }
    )};
    
    # Connect error
    croak $@ if $@;
    
    # Database handle
    $self->dbh($dbh);
    
    return $self;
}

sub DESTROY {
    my $self = shift;
    
    # Disconnect
    $self->disconnect if $self->connected;
}

sub connected { ref shift->{dbh} eq 'DBI::db' }

sub disconnect {
    my $self = shift;
    
    if ($self->connected) {
        
        # Disconnect
        $self->dbh->disconnect;
        delete $self->{dbh};
    }
    
    return $self;
}

sub reconnect {
    my $self = shift;
    
    # Reconnect
    $self->disconnect if $self->connected;
    $self->connect;
    
    return $self;
}

sub create_query {
    my ($self, $template) = @_;
    
    my $class = ref $self;
    
    if (ref $template eq 'ARRAY') {
        $template = $template->[1];
    }
    
    # Create query from SQL template
    my $sql_template = $self->sql_template;
    
    # Try to get cached query
    my $cached_query = $class->_query_caches->{"$template"};
    
    # Create query
    my $query;
    if ($cached_query) {
        $query = DBIx::Custom::Query->new(
            sql       => $cached_query->sql,
            columns => $cached_query->columns
        );
    }
    else {
        $query = eval{$sql_template->create_query($template)};
        croak($@) if $@;
        
        $class->_add_query_cache("$template", $query);
    }
    
    # Connect if not
    $self->connect unless $self->connected;
    
    # Prepare statement handle
    my $sth = $self->dbh->prepare($query->{sql});
    
    # Set statement handle
    $query->sth($sth);
    
    return $query;
}

our %VALID_EXECUTE_ARGS = map { $_ => 1 } qw/param filter/;

sub execute{
    my $self  = shift;
    my $query = shift;
    
    # Arguments
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    # Check arguments
    foreach my $name (keys %$args) {
        croak "\"$name\" is invalid name"
          unless $VALID_EXECUTE_ARGS{$name};
    }
    
    my $params = $args->{param} || {};
    
    # First argument is SQL template
    unless (ref $query eq 'DBIx::Custom::Query') {
        my $template;
        
        if (ref $query eq 'ARRAY') {
            $template = $query->[0];
        }
        else { $template = $query }
        
        $query = $self->create_query($template);
    }
    
    my $filter = $args->{filter} || $query->filter || {};
    
    # Create bind value
    my $bind_values = $self->_build_bind_values($query, $params, $filter);
    
    # Execute
    my $sth      = $query->sth;
    my $affected = eval{$sth->execute(@$bind_values)};
    
    # Execute error
    if (my $execute_error = $@) {
        require Data::Dumper;
        my $sql              = $query->{sql} || '';
        my $params_dump      = Data::Dumper->Dump([$params], ['*params']);
        
        croak("$execute_error" . 
              "<Your SQL>\n$sql\n" . 
              "<Your parameters>\n$params_dump");
    }
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        
        # Get result class
        my $result_class = $self->result_class;
        
        # Create result
        my $result = $result_class->new({
            sth             => $sth,
            default_filter  => $self->default_fetch_filter,
            filters         => $self->filters
        });
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
        my $fname = $filter->{$column} || $self->default_query_filter || '';
        
        my $filter_func;
        if ($fname) {
            
            if (ref $fname eq 'CODE') {
                $filter_func = $fname;
            }
            else {
                my $filters = $self->filters;
                croak "Not exists filter \"$fname\"" unless exists $filters->{$fname};
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

our %VALID_INSERT_ARGS = map { $_ => 1 } qw/table param append filter/;

sub insert {
    my $self = shift;
    
    # Arguments
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};

    # Check arguments
    foreach my $name (keys %$args) {
        croak "\"$name\" is invalid name"
          unless $VALID_INSERT_ARGS{$name};
    }
    
    # Arguments
    my $table  = $args->{table} || '';
    my $param  = $args->{param} || {};
    my $append = $args->{append} || '';
    my $filter = $args->{filter};
    
    # Insert keys
    my @insert_keys = keys %$param;
    
    # Not exists insert keys
    croak("Key-value pairs for insert must be specified to 'insert' second argument")
      unless @insert_keys;
    
    # Templte for insert
    my $template = "insert into $table {insert " . join(' ', @insert_keys) . '}';
    $template .= " $append" if $append;
    
    # Execute query
    my $ret_val = $self->execute($template, param  => $param, 
                                            filter => $filter);
    
    return $ret_val;
}

our %VALID_UPDATE_ARGS
  = map { $_ => 1 } qw/table param where append filter allow_update_all/;

sub update {
    my $self = shift;

    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    # Check arguments
    foreach my $name (keys %$args) {
        croak "\"$name\" is invalid name"
          unless $VALID_UPDATE_ARGS{$name};
    }
    
    # Arguments
    my $table            = $args->{table} || '';
    my $param            = $args->{param} || {};
    my $where            = $args->{where} || {};
    my $append_statement = $args->{append} || '';
    my $filter           = $args->{filter};
    my $allow_update_all = $args->{allow_update_all};
    
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
    my $template = "update $table $update_clause $where_clause";
    $template .= " $append_statement" if $append_statement;
    
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
    my $ret_val = $self->execute($template, param  => $param, 
                                            filter => $filter);
    
    return $ret_val;
}

sub update_all {
    my $self = shift;;
    
    # Arguments
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
    # Allow all update
    $args->{allow_update_all} = 1;
    
    # Update all rows
    return $self->update($args);
}

our %VALID_DELETE_ARGS
  = map { $_ => 1 } qw/table where append filter allow_delete_all/;

sub delete {
    my $self = shift;
    
    # Arguments
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    # Check arguments
    foreach my $name (keys %$args) {
        croak "\"$name\" is invalid name"
          unless $VALID_DELETE_ARGS{$name};
    }
    
    # Arguments
    my $table            = $args->{table} || '';
    my $where            = $args->{where} || {};
    my $append_statement = $args->{append};
    my $filter           = $args->{filter};
    my $allow_delete_all = $args->{allow_delete_all};
    
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
    my $template = "delete from $table $where_clause";
    $template .= " $append_statement" if $append_statement;
    
    # Execute query
    my $ret_val = $self->execute($template, param  => $where, 
                                            filter => $filter);
    
    return $ret_val;
}

sub delete_all {
    my $self = shift;
    
    # Arguments
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    # Allow all delete
    $args->{allow_delete_all} = 1;
    
    # Delete all rows
    return $self->delete($args);
}

our %VALID_SELECT_ARGS
  = map { $_ => 1 } qw/table column where append filter/;

sub select {
    my $self = shift;;
    
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    # Check arguments
    foreach my $name (keys %$args) {
        croak "\"$name\" is invalid name"
          unless $VALID_SELECT_ARGS{$name};
    }
    
    # Arguments
    my $tables = $args->{table} || [];
    $tables = [$tables] unless ref $tables eq 'ARRAY';
    my $columns          = $args->{column} || [];
    my $where_params     = $args->{where} || {};
    my $append_statement = $args->{append} || '';
    my $filter    = $args->{filter};
    
    # SQL template for select statement
    my $template = 'select ';
    
    # Join column clause
    if (@$columns) {
        foreach my $column (@$columns) {
            $template .= "$column, ";
        }
        $template =~ s/, $/ /;
    }
    else {
        $template .= '* ';
    }
    
    # Join table
    $template .= 'from ';
    foreach my $table (@$tables) {
        $template .= "$table, ";
    }
    $template =~ s/, $/ /;
    
    # Where clause keys
    my @where_keys = keys %$where_params;
    
    # Join where clause
    if (@where_keys) {
        $template .= 'where ';
        foreach my $where_key (@where_keys) {
            $template .= "{= $where_key} and ";
        }
    }
    $template =~ s/ and $//;
    
    # Append something to last of statement
    if ($append_statement =~ s/^where //) {
        if (@where_keys) {
            $template .= " and $append_statement";
        }
        else {
            $template .= " where $append_statement";
        }
    }
    else {
        $template .= " $append_statement";
    }
    
    # Execute query
    my $result = $self->execute($template, param  => $where_params, 
                                           filter => $filter);
    
    return $result;
}

sub _add_query_cache {
    my ($class, $template, $query) = @_;
    
    # Query information
    my $query_cache_keys = $class->_query_cache_keys;
    my $query_caches     = $class->_query_caches;
    
    # Already cached
    return $class if $query_caches->{$template};
    
    # Cache
    $query_caches->{$template} = $query;
    push @$query_cache_keys, $template;
    
    # Check cache overflow
    my $overflow = @$query_cache_keys - $class->query_cache_max;
    for (my $i = 0; $i < $overflow; $i++) {
        my $template = shift @$query_cache_keys;
        delete $query_caches->{$template};
    }
    
    return $class;
}

=head1 NAME

DBIx::Custom - DBI with hash bind and filtering system 

=head1 VERSION

Version 0.1501

=cut

our $VERSION = '0.1501';
$VERSION = eval $VERSION;

=head1 STATE

This module is not stable. Method name and functionality will be change.

=head1 SYNOPSYS
    
    # Connect
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');
    
    # Insert 
    $dbi->insert(table  => 'books',
                 param  => {title => 'perl', author => 'Ken'}
                 filter => {title => 'encode_utf8'});
    
    # Update 
    $dbi->update(table  => 'books', 
                 param  => {title => 'aaa', author => 'Ken'}, 
                 where  => {id => 5}
                 filter => {title => 'encode_utf8');
    
    # Update all
    $dbi->update_all(table  => 'books',
                     param  => {title => 'aaa'}
                     filter => {title => 'encode_utf8'});
    
    # Delete
    $dbi->delete(table  => 'books',
                 where  => {author => 'Ken'}
                 filter => {title => 'encode_utf8'});
    
    # Delete all
    $dbi->delete_all(table => 'books');
    
    # Select
    my $result = $dbi->select(table => 'books');
    
    # Select(more complex)
    my $result = $dbi->select(
        'books',
        {
            columns => [qw/author title/],
            where   => {author => 'Ken'},
            append  => 'order by id limit 1',
            filter  => {tilte => 'encode_utf8'}
        }
    );

    # Execute SQL
    $dbi->execute("select title from books");
    
    # Execute SQL with parameters and filter
    $dbi->execute("select id from books where {= author} && {like title}",
                  param  => {author => 'ken', title => '%Perl%'},
                  filter => {tilte => 'encode_utf8'});
    
    # Default filter
    $dbi->default_query_filter('encode_utf8');
    $dbi->default_fetch_filter('decode_utf8');
    
    # Fetch
    while (my $row = $result->fetch) {
        # ...
    }
    
    # Fetch hash
    while (my $row = $result->fetch_hash) {
        
    }
    
    
=head1 ATTRIBUTES

=head2 user

Database user name
    
    $dbi  = $dbi->user('Ken');
    $user = $dbi->user;
    
=head2 password

Database password
    
    $dbi      = $dbi->password('lkj&le`@s');
    $password = $dbi->password;

=head2 data_source

Database data source
    
    $dbi         = $dbi->data_source("dbi:mysql:dbname=$database");
    $data_source = $dbi->data_source;
    
If you know data source more, See also L<DBI>.

=head2 database

Database name

    $dbi      = $dbi->database('books');
    $database = $dbi->database;

=head2 host

Host name

    $dbi  = $dbi->host('somehost.com');
    $host = $dbi->host;

You can also set IP address like '127.03.45.12'.

=head2 port

Port number

    $dbi  = $dbi->port(1198);
    $port = $dbi->port;

=head2 options

DBI options

    $dbi     = $dbi->options({PrintError => 0, RaiseError => 1});
    $options = $dbi->options;

=head2 sql_template

SQLTemplate object

    $dbi          = $dbi->sql_template(DBIx::Cutom::SQLTemplate->new);
    $sql_template = $dbi->sql_template;

See also L<DBIx::Custom::SQLTemplate>.

=head2 filters

Filters

    $dbi     = $dbi->filters({filter1 => sub { }, filter2 => sub {}});
    $filters = $dbi->filters;
    
This method is generally used to get a filter.

    $filter = $dbi->filters->{encode_utf8};

If you add filter, use register_filter method.

=head2 default_query_filter

Default query filter

    $dbi                  = $dbi->default_query_filter($default_query_filter);
    $default_query_filter = $dbi->default_query_filter

Query filter example
    
    $dbi->register_filter(encode_utf8 => sub {
        my $value = shift;
        
        require Encode 'encode_utf8';
        
        return encode_utf8($value);
    });
    
    $dbi->default_query_filter('encode_utf8')

Bind filter arguemts is

    1. $value : Value
    3. $dbi   : DBIx::Custom instance

=head2 default_fetch_filter

Fetching filter

    $dbi                  = $dbi->default_fetch_filter($default_fetch_filter);
    $default_fetch_filter = $dbi->default_fetch_filter;

Fetch filter example

    $dbi->register_filter(decode_utf8 => sub {
        my $value = shift;
        
        require Encode 'decode_utf8';
        
        return decode_utf8($value);
    });

    $dbi->default_fetch_filter('decode_utf8');

Fetching filter arguemts is

    1. Value
    2. DBIx::Custom instance

=head2 result_class

Resultset class

    $dbi          = $dbi->result_class('DBIx::Custom::Result');
    $result_class = $dbi->result_class;

Default is L<DBIx::Custom::Result>

=head2 dbh

Database handle
    
    $dbi = $dbi->dbh($dbh);
    $dbh = $dbi->dbh;
    
=head2 query_cache_max

Query cache max

    $class           = DBIx::Custom->query_cache_max(50);
    $query_cache_max = DBIx::Custom->query_cache_max;

Default value is 50

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 auto_commit

Set and Get auto commit

    $self        = $dbi->auto_commit($auto_commit);
    $auto_commit = $dbi->auto_commit;
    
=head2 connect

Connect to database

    $dbi->connect;

=head2 disconnect

Disconnect database

    $dbi->disconnect;

If database is already disconnected, this method do nothing.

=head2 reconnect

Reconnect to database

    $dbi->reconnect;

=head2 connected

Check if database is connected.
    
    $is_connected = $dbi->connected;
    
=head2 register_filter

Resister filter
    
    $dbi->register_filter($fname1 => $filter1, $fname => $filter2);
    
register_filter example

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

=head2 create_query
    
Create Query object parsing SQL template

    my $query = $dbi->create_query("select * from authors where {= name} and {= age}");

$query is <DBIx::Query> instance. This is executed by query method as the following

    $dbi->execute($query, $params);

If you know SQL template, see also L<DBIx::Custom::SQLTemplate>.

=head2 execute

Query

    $result = $dbi->execute($template, $params);

The following is query example

    $result = $dbi->execute("select * from authors where {= name} and {= age}", 
                            {name => 'taro', age => 19});
    
    while (my @row = $result->fetch) {
        # do something
    }

If you now syntax of template, See also L<DBIx::Custom::SQLTemplate>

execute() return L<DBIx::Custom::Result> instance

=head2 insert

Insert row

    $affected = $dbi->insert(table  => $table, 
                             param  => {%param},
                             append => $append,
                             filter => {%filter});

Retrun value is affected rows count
    
Example

    # insert
    $dbi->insert(table  => 'books', 
                 param  => {title => 'Perl', author => 'Taro'},
                 append => "some statement",
                 filter => {title => 'encode_utf8'})

=head2 update

Update rows

    $affected = $dbi->update(table  => $table, 
                             param  => {%params},
                             where  => {%where},
                             append => $append,
                             filter => {%filter})

Retrun value is affected rows count

Example

    #update
    $dbi->update(table  => 'books',
                 param  => {title => 'Perl', author => 'Taro'},
                 where  => {id => 5},
                 append => "some statement",
                 filter => {title => 'encode_utf8'})

=head2 update_all

Update all rows

    $affected = $dbi->update_all(table  => $table, 
                                 param  => {%params},
                                 filter => {%filter},
                                 append => $append);

Retrun value is affected rows count

Example

    # update_all
    $dbi->update_all(table  => 'books', 
                     param  => {author => 'taro'},
                     filter => {author => 'encode_utf8'});

=head2 delete

Delete rows

    # delete
    $affected = $dbi->delete(table  => $table,
                             where  => {%where},
                             append => $append
                             filter => {%filter});

Retrun value is affected rows count
    
Example

    # delete
    $dbi->delete(table  => 'books',
                 where  => {id => 5},
                 append => 'some statement',
                 filter => {id => 'encode_utf8');

=head2 delete_all

Delete all rows

    $affected = $dbi->delete_all(table => $table);

Retrun value is affected rows count

Example
    
    # delete_all
    $dbi->delete_all('books');

=head2 select
    
Select rows

    $result = $dbi->select(table  => $table,
                           column => [@column],
                           where  => {%where},
                           append => $append,
                           filter => {%filter});

$reslt is L<DBIx::Custom::Result> instance

The following is some select examples

    # select
    $result = $dbi->select('books');
    
    # select * from books where title = 'Perl';
    $result = $dbi->select('books', {title => 1});
    
    # select title, author from books where id = 1 for update;
    $result = $dbi->select(
        table  => 'books',
        where  => ['title', 'author'],
        where  => {id => 1},
        appned => 'for update'
    );

You can join multi tables
    
    $result = $dbi->select(
        ['table1', 'table2'],                # tables
        ['table1.id as table1_id', 'title'], # columns (alias is ok)
        {table1.id => 1},                    # where clase
        "where table1.id = table2.id",       # join clause (must start 'where')
    );

=head1 DBIx::Custom default configuration

By default, "AutoCommit" and "RaiseError" is true.

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
