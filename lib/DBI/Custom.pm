package DBI::Custom;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';
use DBI;
use DBI::Custom::Query;
use DBI::Custom::Result;
use DBI::Custom::SQL::Template;


### Class-Object Accessors
sub user        : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub password    : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub data_source : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub dbi_options : ClassObjectAttr { initialize => {clone => 'hash', 
                                                   default => sub { {} } } }

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
        default => 'DBI::Custom::Result'
    }
}

sub sql_template : ClassObjectAttr {
    initialize => {
        clone   => sub {$_[0] ? $_[0]->clone : undef},
        default => sub {DBI::Custom::SQL::Template->new}
    }
}

### Object Accessor
sub dbh          : Attr {}


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
        
        $query = DBI::Custom::Query->new($query);
        
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
                                 $bind_filter->($original_key, 
                                                $root_params->[$current_key->[0]],
                                                $table, $column);
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
                                 $bind_filter->($original_key,
                                                $root_params->{$current_key}, 
                                                $table, $column);
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

# Run tranzaction
sub run_tranzaction {
    my ($self, $tranzaction) = @_;
    
    # Check auto commit
    croak("AutoCommit must be true before tranzaction start")
      unless $self->_auto_commit;
    
    # Auto commit off
    $self->_auto_commit(0);
    
    # Run tranzaction
    eval {$tranzaction->()};
    
    # Tranzaction error
    my $tranzaction_error = $@;
    
    # Tranzaction is failed.
    if ($tranzaction_error) {
        # Rollback
        eval{$self->dbh->rollback};
        
        # Rollback error
        my $rollback_error = $@;
        
        # Auto commit on
        $self->_auto_commit(1);
        
        if ($rollback_error) {
            # Rollback is failed
            croak("${tranzaction_error}Rollback is failed : $rollback_error");
        }
        else {
            # Rollback is success
            croak("${tranzaction_error}Rollback is success");
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
    
    # Not connected
    croak("Not yet connect to database")
      unless $self->connected;
    
    return $self->dbh->last_insert_id(@_);
}

# Insert
sub insert {
    my ($self, $table, $insert_params, $query_edit_cb) = @_;
    $table         ||= '';
    $insert_params ||= {};
    
    # Insert keys
    my @insert_keys = keys %$insert_params;
    
    # Not exists insert keys
    croak("Key-value pairs for insert must be specified to 'insert' second argument")
      unless @insert_keys;
    
    # Templte for insert
    my $template = "insert into $table {insert " . join(' ', @insert_keys) . '}';
    
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
    my ($self, $table, $update_params,
        $where_params, $query_edit_cb, $options) = @_;
    
    $table         ||= '';
    $update_params ||= {};
    $where_params  ||= {};
    
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
    my ($self, $table, $update_params, $query_edit_cb) = @_;
    
    return $self->update($table, $update_params, {}, $query_edit_cb,
                         {allow_update_all => 1});
}

# Delete
sub delete {
    my ($self, $table, $where_params, $query_edit_cb, $options) = @_;
    $table        ||= '';
    $where_params ||= {};
    
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
    my ($self, $table) = @_;
    return $self->delete($table, {}, undef, {allow_delete_all => 1});
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

Object::Simple->build_class;

=head1 NAME

DBI::Custom - Customizable simple DBI

=head1 VERSION

Version 0.0101

=head1 CAUTION

This module is now experimental stage.

I want you to try this module
because I want this module stable, and not to damage your DB data by this module bug.

Please tell me bug if you find

=head1 SYNOPSIS

  my $dbi = DBI::Custom->new;
  
  my $query = $dbi->create_query($template);
  $dbi->execute($query);

=head1 CLASS-OBJECT ACCESSORS

=head2 user

    # Set and get database user name
    $self = $dbi->user($user);
    $user = $dbi->user;
    
    # Sample
    $dbi->user('taro');

=head2 password

    # Set and get database password
    $self     = $dbi->password($password);
    $password = $dbi->password;
    
    # Sample
    $dbi->password('lkj&le`@s');

=head2 data_source

    # Set and get database data source
    $self        = $dbi->data_source($data_soruce);
    $data_source = $dbi->data_source;
    
    # Sample(SQLite)
    $dbi->data_source(dbi:SQLite:dbname=$database);
    
    # Sample(MySQL);
    $dbi->data_source("dbi:mysql:dbname=$database");
    
    # Sample(PostgreSQL)
    $dbi->data_source("dbi:Pg:dbname=$database");
    
=head2 database

    # Set and get database name
    $self     = $dbi->database($database);
    $database = $dbi->database;

=head2 dbi_options

    # Set and get DBI option
    $self       = $dbi->dbi_options({$options => $value, ...});
    $dbi_options = $dbi->dbi_options;

    # Sample
    $dbi->dbi_options({PrintError => 0, RaiseError => 1});

dbi_options is used when you connect database by using connect.

=head2 prepare

    $sth = $dbi->prepare($sql);

This method is same as DBI::prepare

=head2 do

    $dbi->do($sql, @bind_values);

This method is same as DBI::do

=head2 sql_template

    # Set and get SQL::Template object
    $self         = $dbi->sql_template($sql_template);
    $sql_template = $dbi->sql_template;
    
    # Sample
    $dbi->sql_template(DBI::Cutom::SQL::Template->new);

=head2 filters

    # Set and get filters
    $self    = $dbi->filters($filters);
    $filters = $dbi->filters;

=head2 formats

    # Set and get formats
    $self    = $dbi->formats($formats);
    $formats = $dbi->formats;
    
=head2 bind_filter

    # Set and get binding filter
    $self        = $dbi->bind_filter($bind_filter);
    $bind_filter = $dbi->bind_filter

    # Sample
    $dbi->bind_filter($self->filters->{default_bind_filter});
    

you can get DBI database handle if you need.

=head2 fetch_filter

    # Set and get Fetch filter
    $self         = $dbi->fetch_filter($fetch_filter);
    $fetch_filter = $dbi->fetch_filter;

    # Sample
    $dbi->fetch_filter($self->filters->{default_fetch_filter});

=head2 no_bind_filters

    # Set and get no filter keys when binding
    $self            = $dbi->no_bind_filters($no_bind_filters);
    $no_bind_filters = $dbi->no_bind_filters;

=head2 no_fetch_filters

    # Set and get no filter keys when fetching
    $self             = $dbi->no_fetch_filters($no_fetch_filters);
    $no_fetch_filters = $dbi->no_fetch_filters;

=head2 result_class

    # Set and get resultset class
    $self         = $dbi->result_class($result_class);
    $result_class = $dbi->result_class;
    
    # Sample
    $dbi->result_class('DBI::Custom::Result');

=head2 dbh

    # Get database handle
    $dbh = $self->dbh;

=head1 METHODS

=head2 connect

    # Connect to database
    $self = $dbi->connect;
    
    # Sample
    $dbi = DBI::Custom->new(user => 'taro', password => 'lji8(', 
                            data_soruce => "dbi:mysql:dbname=$database");
    $dbi->connect;

=head2 disconnect

    # Disconnect database
    $dbi->disconnect;

If database is already disconnected, this method do noting.

=head2 reconnect

    # Reconnect
    $dbi->reconnect;

=head2 connected

    # Check connected
    $dbi->connected

=head2 add_filter

    # Add filter (hash ref or hash can be recieve)
    $self = $dbi->add_filter({$filter_name => $filter, ...});
    $self = $dbi->add_filter($filetr_name => $filter, ...);
    
    # Sample
    $dbi->add_filter(
        decode_utf8 => sub {
            my ($key, $value, $table, $column) = @_;
            return Encode::decode('UTF-8', $value);
        },
        datetime_to_string => sub {
            my ($key, $value, $table, $column) = @_;
            return $value->strftime('%Y-%m-%d %H:%M:%S')
        },
        default_bind_filter => sub {
            my ($key, $value, $table, $column) = @_;
            if (ref $value eq 'Time::Piece') {
                return $dbi->filters->{datetime_to_string}->($value);
            }
            else {
                return $dbi->filters->{decode_utf8}->($value);
            }
        },
        
        encode_utf8 => sub {
            my ($key, $value) = @_;
            return Encode::encode('UTF-8', $value);
        },
        string_to_datetime => sub {
            my ($key, $value) = @_;
            return DateTime::Format::MySQL->parse_datetime($value);
        },
        default_fetch_filter => sub {
            my ($key, $value, $type, $sth, $i) = @_;
            if ($type eq 'DATETIME') {
                return $dbi->filters->{string_to_datetime}->($value);
            }
            else {
                return $dbi->filters->{encode_utf8}->($value);
            }
        }
    );

add_filter add filter to filters

=head2 add_format

    $dbi->add_format(date => '%Y:%m:%d');

=head2 create_query
    
    # Create Query object from SQL template
    my $query = $dbi->create_query($template);
    
=head2 execute

    # Parse SQL template and execute SQL
    $result = $dbi->query($query, $params);
    $result = $dbi->query($template, $params); # Shorcut
    
    # Sample
    $result = $dbi->query("select * from authors where {= name} and {= age}", 
                          {author => 'taro', age => 19});
    
    while (my @row = $result->fetch) {
        # do something
    }

See also L<DBI::Custom::SQL::Template>

=head2 run_tranzaction

    # Run tranzaction
    $dbi->run_tranzaction(sub {
        # do something
    });

If tranzaction is success, commit is execute. 
If tranzation is died, rollback is execute.

=head2 insert

    # Insert
    $dbi->insert($table, $insert_values);
    
    # Sample
    $dbi->insert('books', {title => 'Perl', author => 'Taro'});

=head2 update

    # Update
    $dbi->update($table, $update_values, $where);
    
    # Sample
    $dbi->update('books', {title => 'Perl', author => 'Taro'}, {id => 5});

=head2 update_all

    # Update all rows
    $dbi->update($table, $updat_values);

=head2 delete

    # Delete
    $dbi->delete($table, $where);
    
    # Sample
    $dbi->delete('Books', {id => 5});

=head2 delete_all

    # Delete all rows
    $dbi->delete_all($table);

=head2 last_insert_id

    # Get last insert id
    $last_insert_id = $dbi->last_insert_id;
    
This method is same as DBI last_insert_id;

=head2 select
    
    # Select
    $dbi->select(
        $table,                # must be string or array;
        [@$columns],           # must be array reference. this is optional
        {%$where_params},      # must be hash reference.  this is optional
        $append_statement,     # must be string.          this is optional
        $query_edit_callback   # must be code reference.  this is optional
    );
    
    # Sample
    $dbi->select(
        'Books',
        ['title', 'author'],
        {id => 1},
        "for update",
        sub {
            my $query = shift;
            $query->bind_filter(sub {
                # ...
            });
        }
    );
    
    # The way to join multi tables
    $dbi->select(
        ['table1', 'table2'],
        ['table1.id as table1_id', 'title'],
        {table1.id => 1},
        "where table1.id = table2.id",
    );

=head1 Class Accessors

=head2 query_cache_max

    # Max query cache count
    $class           = $class->query_cache_max($query_cache_max);
    $query_cache_max = $class->query_cache_max;
    
    # Sample
    DBI::Custom->query_cache_max(50);

=head1 CAUTION

DBI::Custom have DIB object internal.
This module is work well in the following DBI condition.

    1. AutoCommit is true
    2. RaiseError is true

By default, Both AutoCommit and RaiseError is true.
You must not change these mode not to damage your data.

If you change these mode, 
you cannot get correct error message, 
or run_tranzaction may fail.

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
