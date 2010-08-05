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
    if (!$self->data_source && (my $database = $self->database)) {
        $self->data_source("dbi:SQLite:dbname=$database");
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

sub last_insert_rowid { shift->dbh->func('last_insert_rowid') }

1;

=head1 NAME

DBIx::Custom::SQLite - SQLite implementation

=head1 SYNOPSYS

    use DBIx::Custom::SQLite;
    
    # Connect to the database
    my $dbi = DBIx::Custom::SQLite->connect(database  => 'dbname');
    
    # Connect to the memory database
    my $dbi = DBIx::Custom::SQLite->connect_memory;
    
    # Get last insert row id
    my $id = $dbi->last_insert_rowid;
    
=head1 ATTRIBUTES

L<DBIx::Custom::SQLite> inherits all attributes from L<DBIx::Custom>
and implements the following new ones.

=head2 C<database>

    my $database = $dbi->database;
    $dbi         = $dbi->database('dbname');

Database name.
C<connect()> method use this value to connect the database
if C<data_source> is not specified.

=head1 METHODS

L<DBIx::Custom::SQLite> inherits all methods from L<DBIx::Custom>
and implements the following new ones.

=head2 C<connect>
    
    my $dbi = DBIx::Custom::SQLite->connect(database  => 'dbname');

Create a new L<DBIx::Custom::SQLite> object and connect to database.
This method override C<DBIx::Custom::connect()> method.
You can specify all attributes of L<DBIx::Custom>
and L<DBIx::Custom::SQLite>, such as C<database>.

=head2 C<connect_memory>

    my $dbi = DBIx::Custom::SQLite->connect_memory;

Create a new L<DBIx::Custom::SQLite> object and connect to the memory database.

=head2 C<last_insert_rowid>

    my $last_insert_rowid = $dbi->last_insert_rowid;

Get last insert row id.
This is same as C<last_insert_rowid()> function in SQLite.

=cut
