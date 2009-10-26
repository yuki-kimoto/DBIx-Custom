package DBI::Custom;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';
use DBI;
use DBI::Custom::SQL::Template;
use DBI::Custom::Result;

### Class-Object Accessors
sub connect_info : ClassObjectAttr {
    type => 'hash',
    initialize => {
        clone => sub {
            my $value = shift;
            my $new_value = \%{$value || {}};
            $new_value->{options} = $value->{options} if $value->{options};
            return $new_value;
        },
        default => sub { {} },
    }
}

sub bind_filter  : ClassObjectAttr {
    initialize => {clone => 'scalar'}
}

sub fetch_filter : ClassObjectAttr {
    initialize => {clone => 'scalar'}
}

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
        clone   => sub {my $value = shift; $value ? $value->clone : undef},
        default => sub {DBI::Custom::SQL::Template->new}
    }
}

sub valid_connect_info : ClassObjectAttr {
    type => 'hash',
    deref => 1,
    initialize => {
        clone => 'hash',
        default => sub { return {map {$_ => 1} qw/data_source user password options/} },
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
}

# Auto commit
sub auto_commit {
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
    my $connect_info = $self->connect_info;
    
    foreach my $key (keys %{$self->connect_info}) {
        croak("connect_info '$key' is wrong name")
          unless $self->valid_connect_info->{$key};
    }
    
    my $dbh = DBI->connect(
        $connect_info->{data_source},
        $connect_info->{user},
        $connect_info->{password},
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            %{$connect_info->{options} || {} }
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
    
    $self->auto_commit(0);
    
    eval {
        $tranzaction->();
        $self->dbh->commit;
    };
    
    if ($@) {
        my $tranzaction_error = $@;
        
        $self->dbh->rollback or croak("$@ and rollback also failed");
        croak("$tranzaction_error");
    }
    $self->auto_commit(1);
}

# Create SQL from SQL template
sub create_sql {
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
    
    my ($sql, @bind) = $self->create_sql($template, $values, $filter);
    
    $self->connect unless $self->connected;
    
    my $sth = $self->dbh->prepare($sql);
    
    if ($sth_options) {
        foreach my $key (keys %$sth_options) {
            $sth->{$key} = $sth_options->{$key};
        }
    }
    
    # Execute
    my $ret_val = $sth->execute(@bind);
    
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
    $sth->execute(@bind_values);
    
    return $sth;
}

Object::Simple->build_class;

=head1 NAME

DBI::Custom - Customizable simple DBI

=head1 VERSION

Version 0.0101

=cut

=head1 SYNOPSIS

  my $dbi = DBI::Custom->new;

=head1 METHODS

=head2 add_filter

=head2 bind_filter

=head2 connect

=head2 connect_info

=head2 dbh

=head2 fetch_filter

=head2 filters

=head2 new

=head2 query

=head2 create_sql

=head2 query_raw_sql

=head2 sql_template

=head2 auto_commit

=head2 connected

=head2 disconnect

=head2 reconnect

=head2 result_class

=head2 run_tranzaction

=head2 valid_connect_info


=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of DBI::Custom
