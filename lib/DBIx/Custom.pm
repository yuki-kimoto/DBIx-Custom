package DBIx::Custom;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::SQLTemplate;
use DBIx::Custom::Query;
use Encode qw/encode_utf8 decode_utf8/;

__PACKAGE__->attr('dbh');
__PACKAGE__->attr([qw/user password data_source/]);
__PACKAGE__->attr([qw/default_query_filter default_fetch_filter/]);

__PACKAGE__->dual_attr('filters', default => sub { {} },
                                  inherit => 'hash_copy');
__PACKAGE__->register_filter(
    encode_utf8 => sub { encode_utf8($_[0]) },
    decode_utf8 => sub { decode_utf8($_[0]) }
);

__PACKAGE__->attr(result_class => 'DBIx::Custom::Result');
__PACKAGE__->attr(sql_template => sub { DBIx::Custom::SQLTemplate->new });

sub connect {
    my $proto = shift;
    
    # Create
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Information
    my $data_source = $self->data_source;
    my $user        = $self->user;
    my $password    = $self->password;
    
    # Connect
    my $dbh = eval{DBI->connect(
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

sub disconnect {
    my $self = shift;
    
    # Disconnect
    $self->dbh->disconnect;
    $self->dbh(undef);
    
    return $self;
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
  = map { $_ => 1 } qw/table column where append relation filter param/;

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
    my $columns  = $args->{column} || [];
    my $where    = $args->{where} || {};
    my $relation = $args->{relation};
    my $append   = $args->{append};
    my $filter   = $args->{filter};
    my $param    = $args->{param} || {};
    
    # SQL template for select statement
    my $template = 'select ';
    
    # Column clause
    if (@$columns) {
        foreach my $column (@$columns) {
            $template .= "$column, ";
        }
        $template =~ s/, $/ /;
    }
    else {
        $template .= '* ';
    }
    
    # Table
    $template .= 'from ';
    foreach my $table (@$tables) {
        $template .= "$table, ";
    }
    $template =~ s/, $/ /;
    
    # Where clause
    my @where_keys = keys %$where;
    if (@where_keys) {
        $template .= 'where ';
        foreach my $where_key (@where_keys) {
            $template .= "{= $where_key} and ";
        }
    }
    $template =~ s/ and $//;
    
    # Relation
    if ($relation) {
        $template .= @where_keys ? "and " : "where ";
        foreach my $rkey (keys %$relation) {
            $template .= "$rkey = " . $relation->{$rkey} . " and ";
        }
    }
    $template =~ s/ and $//;
    
    # Append some statement
    $template .= " $append" if $append;
    
    # Execute query
    my $result = $self->execute($template, param  => $where, 
                                           filter => $filter);
    
    return $result;
}

sub create_query {
    my ($self, $template) = @_;
    
    # Create query from SQL template
    my $sql_template = $self->sql_template;
    
    # Get cached query
    my $cache = $self->{_cache}->{$template};
    
    # Create query
    my $query;
    if ($cache) {
        $query = DBIx::Custom::Query->new(
            sql       => $cache->sql,
            columns   => $cache->columns
        );
    }
    else {
        $query = eval{$sql_template->create_query($template)};
        croak($@) if $@;
        
        $self->{_cache}->{$template} = $query
          unless $self->{_cache}->{$template};
    }
    
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

sub register_filter {
    my $invocant = shift;
    
    # Add filter
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

sub auto_commit {
    my $self = shift;
    
    if (@_) {
        
        # Set AutoCommit
        $self->dbh->{AutoCommit} = $_[0];
        
        return $self;
    }
    return $self->dbh->{AutoCommit};
}

sub commit   { shift->dbh->commit }
sub rollback { shift->dbh->rollback }

sub DESTROY {
    my $self = shift;
    
    # Disconnect
    $self->disconnect if $self->dbh;
}

=head1 NAME

DBIx::Custom - DBI with hash parameter binding and filtering system

=head1 VERSION

Version 0.1503

=cut

our $VERSION = '0.1503';
$VERSION = eval $VERSION;

=head1 STABILITY

This module is not stable. Method name and functionality will be change.

=head1 SYNOPSYS
    
    # Connect
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');
    
    # Disconnect
    $dbi->disconnect

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
    my $query = $dbi->create_query(
        "select id from books where {= author} && {like title}"
    );
    $dbi->execute($query, param => {author => 'ken', title => '%Perl%'})
    
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
    
    # DBI instance
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

=item 1. Hash parameter binding.

=item 2. Value filtering.

=item 3. Useful methos such as insert(), update(), delete(), and select().

=back

=head1 ATTRIBUTES

=head2 user

Database user name.
    
    $dbi  = $dbi->user('Ken');
    $user = $dbi->user;
    
=head2 password

Database password.
    
    $dbi      = $dbi->password('lkj&le`@s');
    $password = $dbi->password;

=head2 data_source

Database data source.
    
    $dbi         = $dbi->data_source("dbi:mysql:dbname=$database");
    $data_source = $dbi->data_source;
    
=head2 dbh

Database handle. This is the innstance of L<DBI>
    
    $dbi = $dbi->dbh($dbh);
    $dbh = $dbi->dbh;

You can use all methods of L<DBI>

    my $sth    = $dbi->dbh->prepare("...");
    my $errstr = $dbi->dbh->errstr;
    
=head2 filters

Filters

    $dbi     = $dbi->filters({%filters});
    $filters = $dbi->filters;

encode_utf8 and decode_utf8 is set to this attribute by default.

    $encode_utf8 = $dbi->filters->{encode_utf8};
    $decode_utf8 = $dbi->filters->{decode_utf8};

=head2 default_query_filter

Default query filter.

    $dbi                  = $dbi->default_query_filter('encode_utf8');
    $default_query_filter = $dbi->default_query_filter

=head2 default_fetch_filter

Fetching filter.

    $dbi                  = $dbi->default_fetch_filter('decode_utf8');
    $default_fetch_filter = $dbi->default_fetch_filter;

=head2 result_class

Result class.

    $dbi          = $dbi->result_class('DBIx::Custom::Result');
    $result_class = $dbi->result_class;

L<DBIx::Custom::Result> is set to this attribute by default.

=head2 sql_template

SQLTemplate instance. sql_template attribute must be 
the instance of L<DBIx::Cutom::SQLTemplate> subclass.

    $dbi          = $dbi->sql_template(DBIx::Cutom::SQLTemplate->new);
    $sql_template = $dbi->sql_template;

the instance of DBIx::Cutom::SQLTemplate is set to 
this attribute by default.

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 connect

Connect to database.
    
    my $dbi = DBIx::Custom->connect(data_source => "dbi:mysql:database=books",
                                    user => 'ken', password => '!LFKD%$&');

"AutoCommit" and "RaiseError" option is true, 
and "PrintError" option is false by dfault.

=head2 disconnect

Disconnect database.

    $dbi->disconnect;

If database is already disconnected, this method do nothing.

=head2 insert

Insert row.

    $affected = $dbi->insert(table  => $table, 
                             param  => {%param},
                             append => $append,
                             filter => {%filter});

Retruned value is affected rows count.
    
Example:

    # insert
    $dbi->insert(table  => 'books', 
                 param  => {title => 'Perl', author => 'Taro'},
                 append => "some statement",
                 filter => {title => 'encode_utf8'})

=head2 update

Update rows.

    $affected = $dbi->update(table  => $table, 
                             param  => {%params},
                             where  => {%where},
                             append => $append,
                             filter => {%filter})

Retruned value is affected rows count

Example:

    #update
    $dbi->update(table  => 'books',
                 param  => {title => 'Perl', author => 'Taro'},
                 where  => {id => 5},
                 append => "some statement",
                 filter => {title => 'encode_utf8'});

=head2 update_all

Update all rows.

    $affected = $dbi->update_all(table  => $table, 
                                 param  => {%params},
                                 filter => {%filter},
                                 append => $append);

Retruned value is affected rows count.

Example:

    # update_all
    $dbi->update_all(table  => 'books', 
                     param  => {author => 'taro'},
                     filter => {author => 'encode_utf8'});

=head2 delete

Delete rows.

    $affected = $dbi->delete(table  => $table,
                             where  => {%where},
                             append => $append,
                             filter => {%filter});

Retrun value is affected rows count
    
Example:

    # delete
    $dbi->delete(table  => 'books',
                 where  => {id => 5},
                 append => 'some statement',
                 filter => {id => 'encode_utf8'});

=head2 delete_all

Delete all rows.

    $affected = $dbi->delete_all(table => $table);

Retruned value is affected rows count.

Example:
    
    # delete_all
    $dbi->delete_all(table => 'books');

=head2 select
    
Select rows.

    $result = $dbi->select(table    => $table,
                           column   => [@column],
                           where    => {%where},
                           append   => $append,
                           relation => {%relation},
                           filter   => {%filter});

Return value is the instance of L<DBIx::Custom::Result>.

Example:

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

=head2 create_query
    
Create the instance of L<DBIx::Custom::Query>. 
This receive the string written by SQL template.

    my $query = $dbi->create_query("select * from authors where {= name} and {= age}");

=head2 execute

Execute the instace of L<DBIx::Custom::Query> or
the string written by SQL template.
Return value is the instance of L<DBIx::Custom::Result>.

    $result = $dbi->execute($query,    param => $params, filter => {%filter});
    $result = $dbi->execute($template, param => $params, filter => {%filter});

Example:

    $result = $dbi->execute("select * from authors where {= name} and {= age}", 
                            param => {name => 'taro', age => 19});
    
    while (my $row = $result->fetch) {
        # do something
    }

See also L<DBIx::Custom::SQLTemplate> to know how to write SQL template.

=head2 register_filter

Resister filter.
    
    $dbi->register_filter(%filters);
    
Example:

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

=head2 auto_commit

Auto commit.

    $self        = $dbi->auto_commit(1);
    $auto_commit = $dbi->auto_commit;

This is equal to

    $dbi->dbh->{AutoCommit} = 1;
    $auto_commit = $dbi->dbh->{AutoCommit};

=head2 commit

Commit.

    $dbi->commit;

This is equal to

    $dbi->dbh->commit;

=head2 rollback

Rollback.

    $dbi->rollback

This is equal to

    $dbi->dbh->rollback;

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
