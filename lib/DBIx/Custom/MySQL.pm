package DBIx::Custom::MySQL;

use strict;
use warnings;

use base 'DBIx::Custom';

__PACKAGE__->attr([qw/database host port/]);

sub connect {
    my $proto = shift;
    
    # Create a new object
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Data source
    if (!$self->data_source) {
        my $database = $self->database;
        my $host     = $self->host;
        my $port     = $self->port;
        my $data_source = "dbi:mysql:";
        $data_source .= "database=$database;" if $database;
        $data_source .= "host=$host;"         if $host;
        $data_source .= "port=$port;"         if $port;
        $self->data_source($data_source);
    }
    
    return $self->SUPER::connect;
}

1;

=head1 NAME

DBIx::Custom::MySQL - DEPRECATED!

