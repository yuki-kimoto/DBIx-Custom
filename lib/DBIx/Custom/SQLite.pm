package DBIx::Custom::SQLite;

use strict;
use warnings;

use base 'DBIx::Custom';

use Carp 'croak';

__PACKAGE__->attr('database');

sub connect {
    my $proto = shift;
    
    # Create
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Create data source
    if (!$self->data_source && (my $database = $self->database)) {
        $self->data_source("dbi:SQLite:dbname=$database");
    }
    
    return $self->SUPER::connect;
}

sub connect_memory {
    my $self = shift;
    
    # Data source for memory database
    $self->data_source('dbi:SQLite:dbname=:memory:');
    
    # Already connected
    croak("Already connected") if $self->connected;
    
    # Connect
    $self->connect;
    
    return $self;
}

sub reconnect_memory {
    my $self = shift;

    # Data source for memory database
    $self->data_source('dbi:SQLite:dbname=:memory:');
    
    # Reconnect
    $self->reconnect;
    
    return $self;
}

sub last_insert_rowid {
    my $self = shift;
    
    # Not connected
    croak "Not yet connected" unless $self->connected;
    
    # Get last insert row id
    my $last_insert_rowid = $self->dbh->func('last_insert_rowid');
    
    return $last_insert_rowid;
}

1;

=head1 NAME

DBIx::Custom::SQLite - a SQLite implementation of DBIx::Custom

=head1 SYNOPSYS

    use DBIx::Custom::SQLite;
    
    # Connect
    my $dbi = DBIx::Custom::SQLite->connect(user      => 'taro', 
                                            password => 'kl&@K',
                                            database  => 'your_database');
    
    # Connect memory database
    my $dbi = DBIx::Custom::SQLite->connect_memory;
    
    # Reconnect memory database
    $dbi->reconnect_memory;
    
    # Last insert row ID
    my $id = $dbi->last_insert_rowid;
    
=head1 ATTRIBUTES

This class is L<DBIx::Custom> subclass.
You can use all attributes of L<DBIx::Custom>.

=head2 database

Database name

    $dbi      = $dbi->database('your_database');
    $database = $dbi->database;

=head1 METHODS

This class is L<DBIx::Custom> subclass.
You can use all methods of L<DBIx::Custom>.

=head2 connect - overridden

Connect to database.

    $dbi->connect;

If you set database, host, or port, data source is automatically created.

=head2 connect_memory

Connect memory database.

    $dbi->connect_memory;

=head2 reconnect_memory

Reconnect to memory databsse.

    $dbi->reconnect_memory;

=head2 last_insert_rowid

Last insert row ID.

    $last_insert_rowid = $dbi->last_insert_rowid;
    
This is equal to SQLite last_insert_rowid() function.

=cut
