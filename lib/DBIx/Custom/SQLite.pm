package DBIx::Custom::SQLite;

use strict;
use warnings;

use base 'DBIx::Custom';

__PACKAGE__->attr('database');

sub connect {
    my $proto = shift;
    
    # Create a new object
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Data source
    my $database = $self->database;
    if (!$self->data_source && $database) {
        $self->data_source("dbi:SQLite:dbname=$database")
    }
    
    return $self->SUPER::connect;
}

sub connect_memory {
    my $self = shift->new(@_);
    
    # Data source
    $self->data_source('dbi:SQLite:dbname=:memory:');
    
    # Connect to database
    $self->connect;
    
    return $self;
}

1;

=head1 NAME

DBIx::Custom::SQLite - DEPRECATED!

