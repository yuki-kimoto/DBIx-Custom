package DBI::Custom;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';
use DBI;
use DBI::Custom::SQL::Template;
use DBI::Custom::Result;

### Class-Object Accessors
sub user        : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub password    : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub data_source : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub database    : ClassObjectAttr { initialize => {clone => 'scalar'} }

sub dbi_option : ClassObjectAttr { initialize => {clone => 'hash', 
                                                  default => sub { {} } } }

sub bind_filter  : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub fetch_filter : ClassObjectAttr { initialize => {clone => 'scalar'} }

sub no_filters   : ClassObjectAttr { initialize => {clone => 'array'} }

sub filters : ClassObjectAttr {
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

# Auto commit
sub _auto_commit {
    my $self = shift;
    
    croak("Cannot change AutoCommit becouse of not connected")
        unless $self->dbh;
    
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
    my $dbi_option  = $self->dbi_option;
    
    my $dbh = DBI->connect(
        $data_source,
        $user,
        $password,
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            %{$dbi_option || {} }
        }
    );
    
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
    return exists $self->{dbh} && eval {$self->{dbh}->can('prepare')};
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

# Run tranzaction
sub run_tranzaction {
    my ($self, $tranzaction) = @_;
    
    $self->_auto_commit(0);
    
    eval {
        $tranzaction->();
        $self->dbh->commit;
    };
    
    if ($@) {
        my $tranzaction_error = $@;
        
        $self->dbh->rollback or croak("$@ and rollback also failed");
        croak("$tranzaction_error");
    }
    $self->_auto_commit(1);
}

sub create_query {
    my ($self, $template) = @_;
    
    # Create query from SQL template
    my $query = $self->sql_template->create_query($template);
    
    # Create Query object;
    my $query = DBI::Custom::Query->new($query);
    
    # connect if not
    $self->connect unless $self->connected;
    
    # Prepare statement handle
    my $sth = $self->dbh->prepare($query->{sql});
    
    $query->sth($sth);
    
    return $query;
}

sub execute {
    my ($self, $query, $params)  = @_;
    
    # Create query if First argument is template
    if (!ref $query) {
        my $template = $query;
        $query = $sefl->create_query($tempalte);
    }
    
    # Set bind filter
    $query->bind_filter($self->bind_filter) unless $query->bind_filter;
    
    # Set no filter keys
    $query->no_filters($self->no_filters) unless $query->no_filters;
    
    # Create bind value
    my $bind_values = $self->_build_bind_values($query, $params);
    
    # Execute
    my $ret_val = $query->sth->execute(@$bind_values);
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        my $result_class = $self->result_class;
        my $result = $result_class->new({
            sth => $sth,
            fetch_filter => $self->fetch_filter
        });
        return $result;
    }
    return $ret_val;
}

sub _build_bind_values {
    my ($self, $query, $params) = @_;
    my $bind_filter = $query->bind_filter;
    my $no_filters_map  = $query->_no_filters_map || {};
    
    # binding values
    my @bind_values;
    
    # Filter and sdd bind values
    foreach my $param_key_info (@$param_key_infos) {
        my $filtering_key = $param_key_info->{key};
        my $access_keys = $param_key_info->{access_keys};
        
        my $original_key = $param_key_info->{original_key} || '';
        my $table        = $param_key_info->{table}        || '';
        my $column       = $param_key_info->{column}       || '';
        
        ACCESS_KEYS :
        foreach my $access_key (@$access_keys) {
            my $root_params = $params;
            for (my $i = 0; $i < @$access_key; $i++) {
                my $key = $access_key->[$i];
                
                croak("'access_keys' each value must be string or array reference")
                  unless (ref $key eq 'ARRAY' || ($key && !ref $key));
                
                if ($i == @$access_key - 1) {
                    if (ref $key eq 'ARRAY') {
                        if ($bind_filter && !$no_filters_map->{$original_key}) {
                            push @bind_values, $bind_filter->($root_params->[$key->[0]], $original_key, $table, $column);
                        }
                        else {
                            push @bind_values, scalar $root_params->[$key->[0]];
                        }
                    }
                    else {
                        next ACCESS_KEYS unless exists $root_params->{$key};
                        if ($bind_filter && !$no_filters_map->{$original_key}) {
                            push @bind_values, scalar $bind_filter->($root_params->{$key}, $original_key, $table, $column);
                        }
                        else {
                            push @bind_values, scalar $root_params->{$key};
                        }
                    }
                    return @bind_values;
                }
                
                if ($key eq 'ARRAY') {
                    $root_params = $root_params->[$key->[0]];
                }
                else {
                    next ACCESS_KEYS unless exists $root_params->{$key};
                    $root_params = $root_params->{$key};
                }
            }
        }
        croak("Cannot find key");
    }
}


Object::Simple->build_class;

=head1 NAME

DBI::Custom - Customizable simple DBI

=head1 VERSION

Version 0.0101

=cut

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

=head2 dbi_option

    # Set and get DBI option
    $self       = $dbi->dbi_option({$options => $value, ...});
    $dbi_option = $dbi->dbi_option;

    # Sample
    $dbi->dbi_option({PrintError => 0, RaiseError => 1});

dbi_option is used when you connect database by using connect.

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
            my $value = shift;
            return Encode::decode('UTF-8', $value);
        },
        datetime_to_string => sub {
            my $value = shift;
            return $value->strftime('%Y-%m-%d %H:%M:%S')
        },
        default_bind_filter => sub {
            my ($value, $key, $filters) = @_;
            if (ref $value eq 'Time::Piece') {
                return $filters->{datetime_to_string}->($value);
            }
            else {
                return $filters->{decode_utf8}->($value);
            }
        },
        
        encode_utf8 => sub {
            my $value = shift;
            return Encode::encode('UTF-8', $value);
        },
        string_to_datetime => sub {
            my $value = shift;
            return DateTime::Format::MySQL->parse_datetime($value);
        },
        default_fetch_filter => sub {
            my ($value, $key, $filters, $type, $sth, $i) = @_;
            if ($type eq 'DATETIME') {
                return $self->filters->{string_to_datetime}->($value);
            }
            else {
                return $self->filters->{encode_utf8}->($value);
            }
        }
    );

add_filter add filter to filters

=head2 query

    # Parse SQL template and execute SQL
    $result = $dbi->query($sql_template, $param);
    $result = $dbi->query($sql_template, $param, $bind_filter);
    
    # Sample
    $result = $dbi->query("select * from authors where {= name} && {= age}", 
                          {author => 'taro', age => 19});
    
    while (my @row = $result->fetch) {
        # do something
    }

See also L<DBI::Custom::SQL::Template>

=head2 query_raw_sql

    # Execute SQL
    $result = $dbi->query_raw_sql($sql, @bind_values);
    
    # Sample
    $result = $dbi->query("select * from table where name = ?, 
                          title = ?;", 'taro', 'perl');
    
    while (my @row = $result->fetch) {
        # do something
    }
    
=head2 run_tranzaction

    # Run tranzaction
    $dbi->run_tranzaction(sub {
        # do something
    });

If tranzaction is success, commit is execute. 
If tranzation is died, rollback is execute.

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of DBI::Custom
