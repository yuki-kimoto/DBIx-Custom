package DBIx::Custom::SQLite;

use strict;
use warnings;

use base 'DBIx::Custom::Basic';
use Carp 'croak';

# Add format
__PACKAGE__->add_format(
    datetime => __PACKAGE__->formats->{SQL99_datetime},
    date     => __PACKAGE__->formats->{SQL99_date},
    time     => __PACKAGE__->formats->{SQL99_time},
);

sub connect {
    my $self = shift;
    
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

=head1 NAME

DBIx::Custom::SQLite - DBIx::Custom SQLite implementation

=head1 SYNOPSYS

    use DBIx::Custom::SQLite;
    
    # New
    my $dbi = DBIx::Custom::SQLite->new(user => 'taro', $password => 'kl&@K',
                                        database => 'sample');
    
    # Connect memory database
    my $dbi->connect_memory;
    

=head1 METHODS

This class is L<DBIx::Custom::Basic> subclass.
You can use all methods of L<DBIx::Custom::Basic>

=head2 connect

Connect to database

    $dbi->connect;

If you set database, host, or port, data source is automatically created.

=head2 connect_memory

Connect memory database

    $dbi->connect_memory;

=head2 reconnect_memory

Reconnect to memory databsse

    $dbi->reconnect_memory;

=head2 last_insert_rowid

Get last insert id

    $last_insert_rowid = $dbi->last_insert_rowid;
    
The folloing is last_insert_rowid sample.

    $dbi->insert('books', {title => 'Perl', author => 'taro'});
    $last_insert_rowid = $dbi->last_insert_rowid;

This is equal to SQLite function

    last_insert_rowid()

=cut
