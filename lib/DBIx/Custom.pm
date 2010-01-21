package DBIx::Custom;
use base 'Object::Simple::Base';

use strict;
use warnings;

use 5.008001;

use Carp 'croak';
use DBI;
use DBIx::Custom::Result;
use DBIx::Custom::SQL::Template;

__PACKAGE__->attr('dbh');

__PACKAGE__->class_attr(_query_caches     => sub { {} });
__PACKAGE__->class_attr(_query_cache_keys => sub { [] });
__PACKAGE__->class_attr('query_cache_max', default => 50, clone => 'scalar');

__PACKAGE__->dual_attr([qw/user password data_source/], clone => 'scalar');
__PACKAGE__->dual_attr([qw/database host port/],        clone => 'scalar');
__PACKAGE__->dual_attr([qw/bind_filter fetch_filter/],  clone => 'scalar');

__PACKAGE__->dual_attr([qw/no_bind_filters no_fetch_filters/],
                       default => sub { [] }, clone => 'array');

__PACKAGE__->dual_attr([qw/options filters formats/],
                       default => sub { {} }, clone => 'hash');

__PACKAGE__->dual_attr('result_class', default => 'DBIx::Custom::Result',
                                       clone   => 'scalar');

__PACKAGE__->dual_attr('sql_tmpl', default => sub {DBIx::Custom::SQL::Template->new},
                                   clone   => sub {$_[0] ? $_[0]->clone : undef});

sub add_filter {
    my $invocant = shift;
    
    my $filters = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->filters({%{$invocant->filters}, %$filters});
    
    return $invocant;
}

sub add_format{
    my $invocant = shift;
    
    my $formats = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->formats({%{$invocant->formats}, %$formats});

    return $invocant;
}

sub _auto_commit {
    my $self = shift;
    
    croak("Not yet connect to database") unless $self->dbh;
    
    if (@_) {
        $self->dbh->{AutoCommit} = $_[0];
        return $self;
    }
    return $self->dbh->{AutoCommit};
}

sub connect {
    my $self = shift;
    my $data_source = $self->data_source;
    my $user        = $self->user;
    my $password    = $self->password;
    my $options     = $self->options;
    
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
    
    croak $@ if $@;
    
    $self->dbh($dbh);
    return $self;
}

sub DESTROY {
    my $self = shift;
    $self->disconnect if $self->connected;
}

sub connected {
    my $self = shift;
    return ref $self->{dbh} eq 'DBI::db';
}

sub disconnect {
    my $self = shift;
    if ($self->connected) {
        $self->dbh->disconnect;
        delete $self->{dbh};
    }
}

sub reconnect {
    my $self = shift;
    $self->disconnect if $self->connected;
    $self->connect;
}

sub prepare {
    my ($self, $sql) = @_;
    
    # Connect if not
    $self->connect unless $self->connected;
    
    # Prepare
    my $sth = eval{$self->dbh->prepare($sql)};
    
    # Error
    croak("$@<Your SQL>\n$sql") if $@;
    
    return $sth;
}

sub do{
    my ($self, $sql, @bind_values) = @_;
    
    # Connect if not
    $self->connect unless $self->connected;
    
    # Do
    my $affected = eval{$self->dbh->do($sql, @bind_values)};
    
    # Error
    if ($@) {
        my $error = $@;
        require Data::Dumper;
        
        my $bind_value_dump
          = Data::Dumper->Dump([\@bind_values], ['*bind_valuds']);
        
        croak("$error<Your SQL>\n$sql\n<Your bind values>\n$bind_value_dump\n");
    }
    
    return $affected;
}

sub create_query {
    my ($self, $template) = @_;
    my $class = ref $self;
    
    # Create query from SQL template
    my $sql_tmpl = $self->sql_tmpl;
    
    # Try to get cached query
    my $cached_query = $class->_query_caches->{$template};
    
    # Create query
    my $query;
    if ($query) {
        $query = $self->new(sql       => $cached_query->sql, 
                            key_infos => $cached_query->key_infos);
    }
    else {
        $query = eval{$sql_tmpl->create_query($template)};
        croak($@) if $@;
        
        $class->_add_query_cache($template, $query);
    }
    
    # Connect if not
    $self->connect unless $self->connected;
    
    # Prepare statement handle
    my $sth = $self->prepare($query->{sql});
    
    # Set statement handle
    $query->sth($sth);
    
    # Set bind filter
    $query->bind_filter($self->bind_filter);
    
    # Set no filter keys when binding
    $query->no_bind_filters($self->no_bind_filters);
    
    # Set fetch filter
    $query->fetch_filter($self->fetch_filter);
    
    # Set no filter keys when fetching
    $query->no_fetch_filters($self->no_fetch_filters);
    
    return $query;
}

sub query{
    my ($self, $query, $params)  = @_;
    $params ||= {};
    
    # First argument is SQL template
    if (!ref $query) {
        my $template = $query;
        $query = $self->create_query($template);
        my $query_edit_cb = $_[3];
        $query_edit_cb->($query) if ref $query_edit_cb eq 'CODE';
    }
    
    # Create bind value
    my $bind_values = $self->_build_bind_values($query, $params);
    
    # Execute
    my $sth      = $query->sth;
    my $affected = eval{$sth->execute(@$bind_values)};
    
    # Execute error
    if (my $execute_error = $@) {
        require Data::Dumper;
        my $sql              = $query->{sql} || '';
        my $key_infos_dump   = Data::Dumper->Dump([$query->key_infos], ['*key_infos']);
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
            _dbi             => $self,
            sth              => $sth,
            fetch_filter     => $query->fetch_filter,
            no_fetch_filters => $query->no_fetch_filters
        });
        return $result;
    }
    return $affected;
}

sub _build_bind_values {
    my ($self, $query, $params) = @_;
    my $key_infos           = $query->key_infos;
    my $bind_filter         = $query->bind_filter;
    my $no_bind_filters     = $query->_no_bind_filters || {};
    
    # binding values
    my @bind_values;
    
    # Create bind values
    KEY_INFOS :
    foreach my $key_info (@$key_infos) {
        # Set variable
        my $access_keys  = $key_info->{access_keys};
        my $original_key = $key_info->{original_key} || '';
        my $table        = $key_info->{table}        || '';
        my $column       = $key_info->{column}       || '';
        
        # Key is found?
        my $found;
        
        # Build bind values
        ACCESS_KEYS :
        foreach my $access_key (@$access_keys) {
            # Root parameter
            my $root_params = $params;
            
            # Search corresponding value
            for (my $i = 0; $i < @$access_key; $i++) {
                # Current key
                my $current_key = $access_key->[$i];
                
                # Last key
                if ($i == @$access_key - 1) {
                    # Key is array reference
                    if (ref $current_key eq 'ARRAY') {
                        # Filtering 
                        if ($bind_filter &&
                            !$no_bind_filters->{$original_key})
                        {
                            push @bind_values, 
                                 $bind_filter->($root_params->[$current_key->[0]], 
                                                $original_key, $self,
                                                {table => $table, column => $column});
                        }
                        # Not filtering
                        else {
                            push @bind_values,
                                 scalar $root_params->[$current_key->[0]];
                        }
                    }
                    # Key is string
                    else {
                        # Key is not found
                        next ACCESS_KEYS
                          unless exists $root_params->{$current_key};
                        
                        # Filtering
                        if ($bind_filter &&
                            !$no_bind_filters->{$original_key}) 
                        {
                            push @bind_values,
                                 $bind_filter->($root_params->{$current_key},
                                                $original_key, $self,
                                                {table => $table, column => $column});
                        }
                        # Not filtering
                        else {
                            push @bind_values,
                                 scalar $root_params->{$current_key};
                        }
                    }
                    
                    # Key is found
                    $found = 1;
                    next KEY_INFOS;
                }
                # First or middle key
                else {
                    # Key is array reference
                    if (ref $current_key eq 'ARRAY') {
                        # Go next key
                        $root_params = $root_params->[$current_key->[0]];
                    }
                    # Key is string
                    else {
                        # Not found
                        next ACCESS_KEYS
                          unless exists $root_params->{$current_key};
                        
                        # Go next key
                        $root_params = $root_params->{$current_key};
                    }
                }
            }
        }
        
        # Key is not found
        unless ($found) {
            require Data::Dumper;
            my $key_info_dump  = Data::Dumper->Dump([$key_info], ['*key_info']);
            my $params_dump    = Data::Dumper->Dump([$params], ['*params']);
            croak("Corresponding key is not found in your parameters\n" . 
                  "<Key information>\n$key_info_dump\n\n" .
                  "<Your parameters>\n$params_dump\n");
        }
    }
    return \@bind_values;
}

sub run_transaction {
    my ($self, $transaction) = @_;
    
    # Check auto commit
    croak("AutoCommit must be true before transaction start")
      unless $self->_auto_commit;
    
    # Auto commit off
    $self->_auto_commit(0);
    
    # Run transaction
    eval {$transaction->($self)};
    
    # Tranzaction error
    my $transaction_error = $@;
    
    # Tranzaction is failed.
    if ($transaction_error) {
        # Rollback
        eval{$self->dbh->rollback};
        
        # Rollback error
        my $rollback_error = $@;
        
        # Auto commit on
        $self->_auto_commit(1);
        
        if ($rollback_error) {
            # Rollback is failed
            croak("${transaction_error}Rollback is failed : $rollback_error");
        }
        else {
            # Rollback is success
            croak("${transaction_error}Rollback is success");
        }
    }
    # Tranzaction is success
    else {
        # Commit
        eval{$self->dbh->commit};
        my $commit_error = $@;
        
        # Auto commit on
        $self->_auto_commit(1);
        
        # Commit is failed
        croak($commit_error) if $commit_error;
    }
}

sub last_insert_id {
    my $self = shift;
    my $class = ref $self;
    croak "'$class' do not suppert 'last_insert_id'";
}


sub create_table {
    my ($self, $table, @column_definitions) = @_;
    
    # Create table
    my $sql = "create table $table (\n";
    
    # Column definitions
    foreach my $column_definition (@column_definitions) {
        $sql .= "\t$column_definition,\n";
    }
    $sql =~ s/,\n$//;
    
    # End
    $sql .= "\n);";
    
    # Do query
    return $self->do($sql);
}

sub drop_table {
    my ($self, $table) = @_;
    
    # Drop table
    my $sql = "drop table $table;";

    # Do query
    return $self->do($sql);
}

sub insert {
    my $self             = shift;
    my $table            = shift || '';
    my $insert_params    = shift || {};
    my $append_statement = shift unless ref $_[0];
    my $query_edit_cb    = shift;
    
    # Insert keys
    my @insert_keys = keys %$insert_params;
    
    # Not exists insert keys
    croak("Key-value pairs for insert must be specified to 'insert' second argument")
      unless @insert_keys;
    
    # Templte for insert
    my $template = "insert into $table {insert " . join(' ', @insert_keys) . '}';
    $template .= " $append_statement" if $append_statement;
    # Create query
    my $query = $self->create_query($template);
    
    # Query edit callback must be code reference
    croak("Query edit callback must be code reference")
      if $query_edit_cb && ref $query_edit_cb ne 'CODE';
    
    # Query edit if need
    $query_edit_cb->($query) if $query_edit_cb;
    
    # Execute query
    my $ret_val = $self->query($query, $insert_params);
    
    return $ret_val;
}

sub update {
    my $self             = shift;
    my $table            = shift || '';
    my $update_params    = shift || {};
    my $where_params     = shift || {};
    my $append_statement = shift unless ref $_[0];
    my $query_edit_cb    = shift;
    my $options          = shift;
    
    # Update keys
    my @update_keys = keys %$update_params;
    
    # Not exists update kyes
    croak("Key-value pairs for update must be specified to 'update' second argument")
      unless @update_keys;
    
    # Where keys
    my @where_keys = keys %$where_params;
    
    # Not exists where keys
    croak("Key-value pairs for where clause must be specified to 'update' third argument")
      if !@where_keys && !$options->{allow_update_all};
    
    # Update clause
    my $update_clause = '{update ' . join(' ', @update_keys) . '}';
    
    # Where clause
    my $where_clause = '';
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
    
    # Create query
    my $query = $self->create_query($template);
    
    # Query edit callback must be code reference
    croak("Query edit callback must be code reference")
      if $query_edit_cb && ref $query_edit_cb ne 'CODE';
    
    # Query edit if need
    $query_edit_cb->($query) if $query_edit_cb;
    
    # Rearrange parammeters
    my $params = {'#update' => $update_params, %$where_params};
    
    # Execute query
    my $ret_val = $self->query($query, $params);
    
    return $ret_val;
}

sub update_all {
    my $self             = shift;
    my $table            = shift || '';
    my $update_params    = shift || {};
    my $append_statement = shift unless ref $_[0];
    my $query_edit_cb    = shift;
    my $options          = {allow_update_all => 1};
    
    return $self->update($table, $update_params, {}, $append_statement,
                         $query_edit_cb, $options);
}

sub delete {
    my $self             = shift;
    my $table            = shift || '';
    my $where_params     = shift || {};
    my $append_statement = shift unless ref $_[0];
    my $query_edit_cb    = shift;
    my $options          = shift;
    
    # Where keys
    my @where_keys = keys %$where_params;
    
    # Not exists where keys
    croak("Key-value pairs for where clause must be specified to 'delete' second argument")
      if !@where_keys && !$options->{allow_delete_all};
    
    # Where clause
    my $where_clause = '';
    if (@where_keys) {
        $where_clause = 'where ';
        foreach my $where_key (@where_keys) {
            $where_clause .= "{= $where_key} and ";
        }
        $where_clause =~ s/ and $//;
    }
    
    # Template for delete
    my $template = "delete from $table $where_clause";
    $template .= " $append_statement" if $append_statement;
    
    # Create query
    my $query = $self->create_query($template);
    
    # Query edit callback must be code reference
    croak("Query edit callback must be code reference")
      if $query_edit_cb && ref $query_edit_cb ne 'CODE';
    
    # Query edit if need
    $query_edit_cb->($query) if $query_edit_cb;
    
    # Execute query
    my $ret_val = $self->query($query, $where_params);
    
    return $ret_val;
}

sub delete_all {
    my $self             = shift;
    my $table            = shift || '';
    my $append_statement = shift unless ref $_[0];
    my $query_edit_cb    = shift;
    my $options          = {allow_delete_all => 1};
    
    return $self->delete($table, {}, $append_statement, $query_edit_cb,
                         $options);
}

sub _select_usage { return << 'EOS' }
Your select arguments is wrong.
select usage:
$dbi->select(
    $table,                # must be string or array ref
    [@$columns],           # must be array reference. this can be ommited
    {%$where_params},      # must be hash reference.  this can be ommited
    $append_statement,     # must be string.          this can be ommited
    $query_edit_callback   # must be code reference.  this can be ommited
);
EOS

sub select {
    my $self = shift;
    
    # Check argument
    croak($self->_select_usage) unless @_;
    
    # Arguments
    my $tables = shift || '';
    $tables    = [$tables] unless ref $tables;
    
    my $columns          = ref $_[0] eq 'ARRAY' ? shift : [];
    my $where_params     = ref $_[0] eq 'HASH'  ? shift : {};
    my $append_statement = $_[0] && !ref $_[0]  ? shift : '';
    my $query_edit_cb    = shift if ref $_[0] eq 'CODE';
    
    # Check rest argument
    croak($self->_select_usage) if @_;
    
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
    
    # Create query
    my $query = $self->create_query($template);
    
    # Query edit
    $query_edit_cb->($query) if $query_edit_cb;
    
    # Execute query
    my $result = $self->query($query, $where_params);
    
    return $result;
}

sub _add_query_cache {
    my ($class, $template, $query) = @_;
    my $query_cache_keys = $class->_query_cache_keys;
    my $query_caches     = $class->_query_caches;
    
    return $class if $query_caches->{$template};
    
    $query_caches->{$template} = $query;
    push @$query_cache_keys, $template;
    
    my $overflow = @$query_cache_keys - $class->query_cache_max;
    
    for (my $i = 0; $i < $overflow; $i++) {
        my $template = shift @$query_cache_keys;
        delete $query_caches->{$template};
    }
    
    return $class;
}

sub filter_off {
    my $self = shift;
    
    # filter off
    $self->bind_filter(undef);
    $self->fetch_filter(undef);
    
    return $self;
}

=head1 NAME

DBIx::Custom - Customizable DBI

=head1 VERSION

Version 0.0903

=cut

our $VERSION = '0.0903';

=head1 SYNOPSYS
    
    # New
    my $dbi = DBIx::Custom->new(data_source => "dbi:mysql:database=books"
                                user => 'ken', password => '!LFKD%$&');
    
    # Query
    $dbi->query("select title from books");
    
    # Query with parameters
    $dbi->query("select id from books where {= author} && {like title}",
                {author => 'ken', title => '%Perl%'});
    
    # Insert 
    $dbi->insert('books', {title => 'perl', author => 'Ken'});
    
    # Update 
    $dbi->update('books', {title => 'aaa', author => 'Ken'}, {id => 5});
    
    # Delete
    $dbi->delete('books', {author => 'Ken'});
    
    # Select
    $dbi->select('books');
    $dbi->select('books', {author => 'taro'}); 
    $dbi->select('books', [qw/author title/], {author => 'Ken'});
    $dbi->select('books', [qw/author title/], {author => 'Ken'},
                 'order by id limit 1');

=head1 Accessors

=head2 user

Set and get database user name
    
    $dbi  = $dbi->user('Ken');
    $user = $dbi->user;
    
=head2 password

Set and get database password
    
    $dbi      = $dbi->password('lkj&le`@s');
    $password = $dbi->password;

=head2 data_source

Set and get database data source
    
    $dbi         = $dbi->data_source("dbi:mysql:dbname=$database");
    $data_source = $dbi->data_source;
    
If you know data source more, See also L<DBI>.

=head2 database

Set and get database name

    $dbi      = $dbi->database('books');
    $database = $dbi->database;

=head2 host

Set and get host name

    $dbi  = $dbi->host('somehost.com');
    $host = $dbi->host;

You can also set IP address like '127.03.45.12'.

=head2 port

Set and get port

    $dbi  = $dbi->port(1198);
    $port = $dbi->port;

=head2 options

Set and get DBI option

    $dbi     = $dbi->options({PrintError => 0, RaiseError => 1});
    $options = $dbi->options;

=head2 sql_tmpl

Set and get SQL::Template object

    $dbi      = $dbi->sql_tmpl(DBIx::Cutom::SQL::Template->new);
    $sql_tmpl = $dbi->sql_tmpl;

See also L<DBIx::Custom::SQL::Template>.

=head2 filters

Set and get filters

    $dbi     = $dbi->filters({filter1 => sub { }, filter2 => sub {}});
    $filters = $dbi->filters;
    
This method is generally used to get a filter.

    $filter = $dbi->filters->{encode_utf8};

If you add filter, use add_filter method.

=head2 formats

Set and get formats

    $dbi     = $dbi->formats({format1 => sub { }, format2 => sub {}});
    $formats = $dbi->formats;

This method is generally used to get a format.

    $filter = $dbi->formats->{datetime};

If you add format, use add_format method.

=head2 bind_filter

Set and get binding filter

    $dbi         = $dbi->bind_filter($bind_filter);
    $bind_filter = $dbi->bind_filter

The following is bind filter sample

    $dbi->bind_filter(sub {
        my ($value, $key, $dbi, $infos) = @_;
        
        # edit $value
        
        return $value;
    });

Bind filter arguemts is

    1. $value : Value
    2. $key   : Key
    3. $dbi   : DBIx::Custom object
    4. $infos : {table => $table, column => $column}

=head2 fetch_filter

Set and get Fetch filter

    $dbi          = $dbi->fetch_filter($fetch_filter);
    $fetch_filter = $dbi->fetch_filter;

The following is fetch filter sample

    $dbi->fetch_filter(sub {
        my ($value, $key, $dbi, $infos) = @_;
        
        # edit $value
        
        return $value;
    });

Bind filter arguemts is

    1. $value : Value
    2. $key   : Key
    3. $dbi   : DBIx::Custom object
    4. $infos : {type => $table, sth => $sth, index => $index}

=head2 no_bind_filters

Set and get no filter keys when binding
    
    $dbi             = $dbi->no_bind_filters(qw/title author/);
    $no_bind_filters = $dbi->no_bind_filters;

=head2 no_fetch_filters

Set and get no filter keys when fetching

    $dbi              = $dbi->no_fetch_filters(qw/title author/);
    $no_fetch_filters = $dbi->no_fetch_filters;

=head2 result_class

Set and get resultset class

    $dbi          = $dbi->result_class('DBIx::Custom::Result');
    $result_class = $dbi->result_class;

=head2 dbh

Get database handle
    
    $dbi = $dbi->dbh($dbh);
    $dbh = $dbi->dbh;
    
=head2 query_cache_max

Set and get query cache max

    $class           = DBIx::Custom->query_cache_max(50);
    $query_cache_max = DBIx::Custom->query_cache_max;

Default value is 50

=head2 Accessor summary

                       Accessor type       Variable type
    user               class and object    scalar(string)
    password           class and object    scalar(string)
    data_source        class and object    scalar(string)
    database           class and object    scalar(string)
    host               class and object    scalar(string)

    port               class and object    scalar(int)
    options            class and object    hash(string)
    sql_tmpl           class and object    scalar(DBIx::Custom::SQL::Template)
    filters            class and object    hash(code ref)
    formats            class and object    hash(string)

    bind_filter        class and object    scalar(code ref)
    fetch_filter       class and object    scalar(code ref)
    no_bind_filters    class and object    array(string)
    no_fetch_filters   class and object    array(string)
    result_class       class and object    scalar(string)

    dbh                object              scalar(DBI)
    query_cache_max    class               scalar(int)

=head1 Methods

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
    
=head2 filter_off

bind_filter and fitch_filter off
    
    $dbi->filter_off
    
This method is equeal to
    
    $dbi->bind_filter(undef);
    $dbi->fetch_filter(undef);

=head2 add_filter

Resist filter
    
    $dbi->add_filter($fname1 => $filter1, $fname => $filter2);
    
The following is add_filter sample

    $dbi->add_filter(
        encode_utf8 => sub {
            my ($value, $key, $dbi, $infos) = @_;
            utf8::upgrade($value) unless Encode::is_utf8($value);
            return encode('UTF-8', $value);
        },
        decode_utf8 => sub {
            my ($value, $key, $dbi, $infos) = @_;
            return decode('UTF-8', $value)
        }
    );

=head2 add_format

Add format

    $dbi->add_format($fname1 => $format, $fname2 => $format2);
    
The following is add_format sample.

    $dbi->add_format(date => '%Y:%m:%d', datetime => '%Y-%m-%d %H:%M:%S');

=head2 create_query
    
Create Query object parsing SQL template

    my $query = $dbi->create_query("select * from authors where {= name} and {= age}");

$query is <DBIx::Query> object. This is executed by query method as the following

    $dbi->query($query, $params);

If you know SQL template, see also L<DBIx::Custom::SQL::Template>.

=head2 query

Query

    $result = $dbi->query($template, $params);

The following is query sample

    $result = $dbi->query("select * from authors where {= name} and {= age}", 
                          {author => 'taro', age => 19});
    
    while (my @row = $result->fetch) {
        # do something
    }

If you now syntax of template, See also L<DBIx::Custom::SQL::Template>

Return value of query method is L<DBIx::Custom::Result> object

See also L<DBIx::Custom::Result>.

=head2 run_transaction

Run transaction

    $dbi->run_transaction(sub {
        my $dbi = shift;
        
        # do something
    });

If transaction is success, commit is execute. 
If tranzation is died, rollback is execute.

=head2 create_table

Create table

    $dbi->create_table(
        'books',
        'name char(255)',
        'age  int'
    );

First argument is table name. Rest arguments is column definition.

=head2 drop_table

Drop table

    $dbi->drop_table('books');

=head2 insert

Insert row

    $affected = $dbi->insert($table, \%$insert_params);
    $affected = $dbi->insert($table, \%$insert_params, $append);

Retrun value is affected rows count
    
The following is insert sample.

    $dbi->insert('books', {title => 'Perl', author => 'Taro'});

You can add statement.

    $dbi->insert('books', {title => 'Perl', author => 'Taro'}, "some statement");

=head2 update

Update rows

    $affected = $dbi->update($table, \%update_params, \%where);
    $affected = $dbi->update($table, \%update_params, \%where, $append);

Retrun value is affected rows count

The following is update sample.

    $dbi->update('books', {title => 'Perl', author => 'Taro'}, {id => 5});

You can add statement.

    $dbi->update('books', {title => 'Perl', author => 'Taro'},
                 {id => 5}, "some statement");

=head2 update_all

Update all rows

    $affected = $dbi->update_all($table, \%updat_params);

Retrun value is affected rows count

The following is update_all sample.

    $dbi->update_all('books', {author => 'taro'});

=head2 delete

Delete rows

    $affected = $dbi->delete($table, \%where);
    $affected = $dbi->delete($table, \%where, $append);

Retrun value is affected rows count
    
The following is delete sample.

    $dbi->delete('books', {id => 5});

You can add statement.

    $dbi->delete('books', {id => 5}, "some statement");

=head2 delete_all

Delete all rows

    $affected = $dbi->delete_all($table);

Retrun value is affected rows count

The following is delete_all sample.

    $dbi->delete_all('books');

=head2 select
    
Select rows

    $resut = $dbi->select(
        $table,                # must be string or array;
        \@$columns,            # must be array reference. this can be ommited
        \%$where_params,       # must be hash reference.  this can be ommited
        $append_statement,     # must be string.          this can be ommited
        $query_edit_callback   # must be code reference.  this can be ommited
    );

$reslt is L<DBIx::Custom::Result> object

The following is some select samples

    # select * from books;
    $result = $dbi->select('books');
    
    # select * from books where title = 'Perl';
    $result = $dbi->select('books', {title => 1});
    
    # select title, author from books where id = 1 for update;
    $result = $dbi->select(
        'books',              # table
        ['title', 'author'],  # columns
        {id => 1},            # where clause
        'for update',         # append statement
    );

You can join multi tables
    
    $result = $dbi->select(
        ['table1', 'table2'],                # tables
        ['table1.id as table1_id', 'title'], # columns (alias is ok)
        {table1.id => 1},                    # where clase
        "where table1.id = table2.id",       # join clause (must start 'where')
    );

You can also edit query
        
    $dbi->select(
        'books',
        # column, where clause, append statement,
        sub {
            my $query = shift;
            $query->bind_filter(sub {
                # ...
            });
        }
    }


=head2 last_insert_id

Get last insert id

    $last_insert_id = $dbi->last_insert_id;

This method is implemented by subclass.

=head2 prepare

Prepare statement handle.

    $sth = $dbi->prepare('select * from books;');

This method is same as DBI prepare method.

See also L<DBI>.

=head2 do

Execute SQL

    $affected = $dbi->do('insert into books (title, author) values (?, ?)',
                        'Perl', 'taro');

Retrun value is affected rows count.

This method is same as DBI do method.

See also L<DBI>

=head1 DBIx::Custom default configuration

DBIx::Custom have DBI object.
This module is work well in the following DBI condition.

    1. AutoCommit is true
    2. RaiseError is true

By default, Both AutoCommit and RaiseError is true.
You must not change these mode not to damage your data.

If you change these mode, 
you cannot get correct error message, 
or run_transaction may fail.

=head1 Inheritance of DBIx::Custom

DBIx::Custom is customizable DBI.
You can inherit DBIx::Custom and custumize attributes.

    package DBIx::Custom::Yours;
    use base DBIx::Custom;
    
    my $class = __PACKAGE__;
    
    $class->user('your_name');
    $class->password('your_password');

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
