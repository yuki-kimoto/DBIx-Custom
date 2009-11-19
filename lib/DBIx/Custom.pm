use 5.008001;

package DBIx::Custom;
use Object::Simple;

our $VERSION = '0.0601';

use Carp 'croak';
use DBI;
use DBIx::Custom::Query;
use DBIx::Custom::Result;
use DBIx::Custom::SQL::Template;


### Accessors
sub user        : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub password    : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub data_source : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub dbi_options : ClassObjectAttr { initialize => {clone => 'hash', 
                                                   default => sub { {} } } }
sub database    : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub host        : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub port        : ClassObjectAttr { initialize => {clone => 'scalar'} }

sub bind_filter  : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub fetch_filter : ClassObjectAttr { initialize => {clone => 'scalar'} }

sub no_bind_filters   : ClassObjectAttr { initialize => {clone => 'array'} }
sub no_fetch_filters  : ClassObjectAttr { initialize => {clone => 'array'} }

sub filters : ClassObjectAttr {
    type => 'hash',
    deref => 1,
    initialize => {
        clone   => 'hash',
        default => sub { {} }
    }
}

sub formats : ClassObjectAttr {
    type => 'hash',
    deref => 1,
    initialize => {
        clone   => 'hash',
        default => sub { {} }
    }
}

sub result_class : ClassObjectAttr {
    initialize => {
        clone   => 'scalar',
        default => 'DBIx::Custom::Result'
    }
}

sub sql_template : ClassObjectAttr {
    initialize => {
        clone   => sub {$_[0] ? $_[0]->clone : undef},
        default => sub {DBIx::Custom::SQL::Template->new}
    }
}

sub dbh : Attr {}


### Methods

# Add filter
sub add_filter {
    my $invocant = shift;
    
    my %old_filters = $invocant->filters;
    my %new_filters = ref $_[0] eq 'HASH' ? %{$_[0]} : @_;
    $invocant->filters(%old_filters, %new_filters);
    return $invocant;
}

# Add format
sub add_format{
    my $invocant = shift;
    
    my %old_formats = $invocant->formats;
    my %new_formats = ref $_[0] eq 'HASH' ? %{$_[0]} : @_;
    $invocant->formats(%old_formats, %new_formats);
    return $invocant;
}

# Auto commit
sub _auto_commit {
    my $self = shift;
    
    croak("Not yet connect to database") unless $self->dbh;
    
    if (@_) {
        $self->dbh->{AutoCommit} = $_[0];
        return $self;
    }
    return $self->dbh->{AutoCommit};
}

# Connect
sub connect {
    my $self = shift;
    my $data_source = $self->data_source;
    my $user        = $self->user;
    my $password    = $self->password;
    my $dbi_options  = $self->dbi_options;
    
    my $dbh = eval{DBI->connect(
        $data_source,
        $user,
        $password,
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            %{$dbi_options || {} }
        }
    )};
    
    croak $@ if $@;
    
    $self->dbh($dbh);
    return $self;
}

# DESTROY
sub DESTROY {
    my $self = shift;
    $self->disconnect if $self->connected;
}

# Is connected?
sub connected {
    my $self = shift;
    return ref $self->{dbh} eq 'DBI::db';
}

# Disconnect
sub disconnect {
    my $self = shift;
    if ($self->connected) {
        $self->dbh->disconnect;
        delete $self->{dbh};
    }
}

# Reconnect
sub reconnect {
    my $self = shift;
    $self->disconnect if $self->connected;
    $self->connect;
}

# Prepare statement handle
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

# Execute SQL directly
sub do{
    my ($self, $sql, @bind_values) = @_;
    
    # Connect if not
    $self->connect unless $self->connected;
    
    # Do
    my $ret_val = eval{$self->dbh->do($sql, @bind_values)};
    
    # Error
    if ($@) {
        my $error = $@;
        require Data::Dumper;
        
        my $bind_value_dump
          = Data::Dumper->Dump([\@bind_values], ['*bind_valuds']);
        
        croak("$error<Your SQL>\n$sql\n<Your bind values>\n$bind_value_dump\n");
    }
}

# Create query
sub create_query {
    my ($self, $template) = @_;
    my $class = ref $self;
    
    # Create query from SQL template
    my $sql_template = $self->sql_template;
    
    # Try to get cached query
    my $query = $class->_query_caches->{$template};
    
    # Create query
    unless ($query) {
        $query = eval{$sql_template->create_query($template)};
        croak($@) if $@;
        
        $query = DBIx::Custom::Query->new($query);
        
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

# Execute query
sub execute {
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
    my $sth = $query->sth;
    my $ret_val = eval{$sth->execute(@$bind_values)};
    
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
    return $ret_val;
}

# Build binding values
sub _build_bind_values {
    my ($self, $query, $params) = @_;
    my $key_infos           = $query->key_infos;
    my $bind_filter         = $query->bind_filter;
    my $no_bind_filters_map = $query->_no_bind_filters_map || {};
    
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
                            !$no_bind_filters_map->{$original_key})
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
                            !$no_bind_filters_map->{$original_key}) 
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

# Run transaction
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

# Get last insert id
sub last_insert_id {
    my $self = shift;
    my $class = ref $self;
    croak "'$class' do not suppert 'last_insert_id'";
}

# Insert
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
    my $ret_val = $self->execute($query, $insert_params);
    
    return $ret_val;
}

# Update
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
    my $ret_val = $self->execute($query, $params);
    
    return $ret_val;
}

# Update all rows
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

# Delete
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
    my $ret_val = $self->execute($query, $where_params);
    
    return $ret_val;
}

# Delete all rows
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
    [@$columns],           # must be array reference. this is optional
    {%$where_params},      # must be hash reference.  this is optional
    $append_statement,     # must be string.          this is optional
    $query_edit_callback   # must be code reference.  this is optional
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
    my $result = $self->execute($query, $where_params);
    
    return $result;
}

sub _query_caches     : ClassAttr { type => 'hash',
                                    auto_build => sub {shift->_query_caches({}) } }
                                    
sub _query_cache_keys : ClassAttr { type => 'array',
                                    auto_build => sub {shift->_query_cache_keys([])} }
                                    
sub query_cache_max   : ClassAttr { auto_build => sub {shift->query_cache_max(50)} }

# Add query cahce
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

# Both bind_filter and fetch_filter off
sub filter_off {
    my $self = shift;
    
    # filter off
    $self->bind_filter(undef);
    $self->fetch_filter(undef);
    
    return $self;
}

Object::Simple->build_class;

=head1 NAME

DBIx::Custom - Customizable simple DBI

=head1 Version

Version 0.0601

=head1 Caution

This module is now experimental stage.

I want you to try this module
because I want this module stable, and not to damage your DB data by this module bug.

Please tell me bug if you find

=head1 Synopsys

  my $dbi = DBIx::Custom->new;
  
  my $query = $dbi->create_query($template);
  $dbi->execute($query);

=head1 Accessors

=head2 user

Set and get database user name
    
    # For object
    $self  = $self->user($user);
    $user  = $self->user;
    
    # For class
    $class = $class->user($user);
    $user  = $class->user;
    
    # Sample
    $dbi->user('taro');
    
=head2 password

Set and get database password
    
    # For object
    $self     = $self->password($password);
    $password = $self->password;

    # For class
    $class    = $class->password($password);
    $password = $class->password;
    
    # Sample
    $dbi->password('lkj&le`@s');

=head2 data_source

Set and get database data source
    
    # For object
    $self        = $self->data_source($data_soruce);
    $data_source = $self->data_source;
    
    # For class
    $class       = $class->data_source($data_soruce);
    $data_source = $class->data_source;
    
    # Sample(SQLite)
    $dbi->data_source(dbi:SQLite:dbname=$database);
    
    # Sample(MySQL);
    $dbi->data_source("dbi:mysql:dbname=$database");
    
    # Sample(PostgreSQL)
    $dbi->data_source("dbi:Pg:dbname=$database");
    
=head2 database

Set and get database name

    # For object
    $self     = $self->database($database);
    $database = $self->database;

    # For class
    $class    = $class->database($database);
    $database = $class->database;
    
    # Sample
    $dbi->database('books');

=head2 host

Set and get host name

    # For object
    $self = $self->host($host);
    $host = $self->host;

    # For class
    $class = $class->host($host);
    $host  = $class->host;
    
    # Sample
    $dbi->host('somehost.com');
    $dbi->host('127.1.2.3');

=head2 port

Set and get port

    # For object
    $self = $self->port($port);
    $port = $self->port;

    # For class
    $class = $class->port($port);
    $port = $class->port;
    
    # Sample
    $dbi->port(1198);

=head2 dbi_options

Set and get DBI option

    # For object
    $self        = $self->dbi_options({$options => $value, ...});
    $dbi_options = $self->dbi_options;
    
    # For class
    $class       = $class->dbi_options({$options => $value, ...});
    $dbi_options = $class->dbi_options;
    
    # Sample
    $dbi->dbi_options({PrintError => 0, RaiseError => 1});

=head2 sql_template

Set and get SQL::Template object

    # For object
    $self         = $self->sql_template($sql_template);
    $sql_template = $self->sql_template;

    # For class
    $class        = $class->sql_template($sql_template);
    $sql_template = $class->sql_template;

    # Sample
    $dbi->sql_template(DBI::Cutom::SQL::Template->new);

=head2 filters

Set and get filters

    # For object
    $self    = $self->filters($filters);
    $filters = $self->filters;

    # For class
    $class   = $class->filters($filters);
    $filters = $class->filters;
    
    # Sample
    $ret = $dbi->filters->{encode_utf8}->($value);

=head2 formats

Set and get formats

    # For object
    $self    = $self->formats($formats);
    $formats = $self->formats;

    # For class
    $self    = $self->formats($formats);
    $formats = $self->formats;

    # Sample
    $datetime_format = $dbi->formats->{datetime};

=head2 bind_filter

Set and get binding filter

    # For object
    $self        = $self->bind_filter($bind_filter);
    $bind_filter = $self->bind_filter

    # For object
    $class       = $class->bind_filter($bind_filter);
    $bind_filter = $class->bind_filter

    # Sample
    $dbi->bind_filter(sub {
        my ($value, $key, $dbi, $infos) = @_;
        
        # edit $value
        
        return $value;
    });

=head2 fetch_filter

Set and get Fetch filter

    # For object
    $self         = $self->fetch_filter($fetch_filter);
    $fetch_filter = $self->fetch_filter;

    # For class
    $class        = $class->fetch_filter($fetch_filter);
    $fetch_filter = $class->fetch_filter;

    # Sample
    $dbi->fetch_filter(sub {
        my ($value, $key, $dbi, $infos) = @_;
        
        # edit $value
        
        return $value;
    });

=head2 no_bind_filters

Set and get no filter keys when binding
    
    # For object
    $self            = $self->no_bind_filters($no_bind_filters);
    $no_bind_filters = $self->no_bind_filters;

    # For class
    $class           = $class->no_bind_filters($no_bind_filters);
    $no_bind_filters = $class->no_bind_filters;

    # Sample
    $dbi->no_bind_filters(qw/title author/);

=head2 no_fetch_filters

Set and get no filter keys when fetching

    # For object
    $self             = $self->no_fetch_filters($no_fetch_filters);
    $no_fetch_filters = $self->no_fetch_filters;

    # For class
    $class            = $class->no_fetch_filters($no_fetch_filters);
    $no_fetch_filters = $class->no_fetch_filters;

    # Sample
    $dbi->no_fetch_filters(qw/title author/);

=head2 result_class

Set and get resultset class

    # For object
    $self         = $dbi->result_class($result_class);
    $result_class = $dbi->result_class;
    
    # For class
    $class        = $class->result_class($result_class);
    $result_class = $class->result_class;
    
    # Sample
    $dbi->result_class('DBIx::Custom::Result');

=head2 dbh

Get database handle
    
    $self = $self->dbh($dbh);
    $dbh  = $self->dbh;
    
    # Sample
    $table_info = $dbi->dbh->table_info
    
=head2 query_cache_max

Set and get query cache max

    $class           = $class->query_cache_max($query_cache_max);
    $query_cache_max = $class->query_cache_max;
    
    # Sample
    DBIx::Custom->query_cache_max(50);

DBIx::Custom cache queries for performance.

Default is 50

=head1 Methods

=head2 connect

Connect to database

    $self = $dbi->connect;
    
    # Sample
    $dbi = DBIx::Custom->new(user => 'taro', password => 'lji8(', 
                            data_soruce => "dbi:mysql:dbname=$database");
    $dbi->connect;

=head2 disconnect

Disconnect database

    $self = $dbi->disconnect;
    
    # Sample
    $dbi->disconnect;

If database is already disconnected, this method do noting.

=head2 reconnect

Reconnect to database

    $self = $dbi->reconnect;
    
    # Sample
    $dbi->reconnect;

=head2 connected

Check connected
    
    $is_connected = $self->connected;
    
    # Sample
    if ($dbi->connected) { # do something }
    
=head2 filter_off

bind_filter and fitch_filter off
    
    $self = $self->filter_off
    
    # Sample
    $dbi->filter_off;
    
This is equeal to
    
    $dbi->bind_filter(undef);
    $dbi->fetch_filter(undef);

=head2 add_filter

Resist filter
    
    $self = $self->add_filter({$name => $filter, ...});
    # or
    $self = $self->add_filter($name => $filter, ...);
    
    # Sample (For example DBIx::Custom::Basic)
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

Resist format

    $self = $self->add_format({$name => $format, ...});
    # or
    $self = $self->add_format($name => $format, ...);
    
    # Sample
    $dbi->add_format(date => '%Y:%m:%d', datetime => '%Y-%m-%d %H:%M:%S');

=head2 prepare

Prepare statement handle

    $sth = $self->prepare($sql);
    
    # Sample
    $sth = $dbi->prepare('select * from books;');

This method is same as DBI prepare method.

=head2 do

Execute SQL

    $ret_val = $self->do($sql, @bind_values);
    
    # Sample
    $ret_val = $dbi->do('insert into books (title, author) values (?, ?)',
                        'Perl', 'taro');

This method is same as DBI do method.

=head2 create_query
    
Create Query object from SQL template

    my $query = $dbi->create_query($template);
    
=head2 execute

Parse SQL template and execute SQL

    $result = $dbi->query($query, $params);
    $result = $dbi->query($template, $params); # Shortcut
    
    # Sample
    $result = $dbi->query("select * from authors where {= name} and {= age}", 
                          {author => 'taro', age => 19});
    
    while (my @row = $result->fetch) {
        # do something
    }

See also L<DBIx::Custom::SQL::Template>

=head2 run_transaction

Run transaction

    $dbi->run_transaction(sub {
        my $dbi = shift;
        
        # do something
    });

If transaction is success, commit is execute. 
If tranzation is died, rollback is execute.

=head2 insert

Insert row

    $ret_val = $self->insert($table, \%$insert_params);

$ret_val is maybe affected rows count
    
    # Sample
    $dbi->insert('books', {title => 'Perl', author => 'Taro'});

=head2 update

Update rows

    $self = $self->update($table, \%update_params, \%where);

$ret_val is maybe affected rows count

    # Sample
    $dbi->update('books', {title => 'Perl', author => 'Taro'}, {id => 5});

=head2 update_all

Update all rows

    $ret_val = $self->update_all($table, \%updat_params);

$ret_val is maybe affected rows count

    # Sample
    $dbi->update_all('books', {author => 'taro'});

=head2 delete

Delete rows

    $ret_val = $self->delete($table, \%where);

$ret_val is maybe affected rows count
    
    # Sample
    $dbi->delete('books', {id => 5});

=head2 delete_all

Delete all rows

    $ret_val = $self->delete_all($table);

$ret_val is maybe affected rows count

    # Sample
    $dib->delete_all('books');

=head2 select
    
Select rows

    $resut = $self->select(
        $table,                # must be string or array;
        \@$columns,            # must be array reference. this is optional
        \%$where_params,       # must be hash reference.  this is optional
        $append_statement,     # must be string.          this is optional
        $query_edit_callback   # must be code reference.  this is optional
    );

$reslt is L<DBI::Custom::Result> object

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

=head1 Caution

DBIx::Custom have DBI object.
This module is work well in the following DBI condition.

    1. AutoCommit is true
    2. RaiseError is true

By default, Both AutoCommit and RaiseError is true.
You must not change these mode not to damage your data.

If you change these mode, 
you cannot get correct error message, 
or run_transaction may fail.

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
