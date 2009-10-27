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

sub dbi_option : ClassObjectAttr { initialize => {clone => 'hash', 
                                                  default => sub { {} } } }

sub bind_filter  : ClassObjectAttr { initialize => {clone => 'scalar'} }
sub fetch_filter : ClassObjectAttr { initialize => {clone => 'scalar'} }

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

# Create SQL from SQL template
sub _create_sql {
    my $self = shift;
    
    my ($sql, @bind) = $self->sql_template->create_sql(@_);
    
    return ($sql, @bind);
}

# Prepare and execute SQL
sub query {
    my ($self, $template, $values, $filter)  = @_;
    
    my $sth_options;
    
    # Rearrange when argumets is hash referecne 
    if (ref $template eq 'HASH') {
        my $args = $template;
        ($template, $values, $filter, $sth_options)
          = @{$args}{qw/template values filter sth_options/};
    }
    
    $filter ||= $self->bind_filter;
    
    my ($sql, @bind_values) = $self->_create_sql($template, $values, $filter);
    
    $self->connect unless $self->connected;
    
    my $sth = $self->dbh->prepare($sql);
    
    if ($sth_options) {
        foreach my $key (keys %$sth_options) {
            $sth->{$key} = $sth_options->{$key};
        }
    }
    
    # Execute
    my $ret_val = $sth->execute(@bind_values);
    
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

# Prepare and execute raw SQL
sub query_raw_sql {
    my ($self, $sql, @bind_values) = @_;
    
    # Connect
    $self->connect unless $self->connected;
    
    # Add semicolon if not exist;
    $sql .= ';' unless $sql =~ /;$/;
    
    # Prepare
    my $sth = $self->dbh->prepare($sql);
    
    # Execute
    my $ret_val = $sth->execute(@bind_values);
    
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

Object::Simple->build_class;

=head1 NAME

DBI::Custom - Customizable simple DBI

=head1 VERSION

Version 0.0101

=cut

=head1 SYNOPSIS

  my $dbi = DBI::Custom->new;

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
