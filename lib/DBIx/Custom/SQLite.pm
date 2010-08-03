package DBIx::Custom::SQLite;

use strict;
use warnings;

use base 'DBIx::Custom';

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
    my $self = shift->new(@_);
    
    # Data source for memory database
    $self->data_source('dbi:SQLite:dbname=:memory:');
    
    # Connect
    $self->connect;
    
    return $self;
}

sub last_insert_rowid { shift->dbh->func('last_insert_rowid') }

1;

=head1 NAME

DBIx::Custom::SQLite - SQLite implementation

=head1 SYNOPSYS

    use DBIx::Custom::SQLite;
    
    # Connect to database
    my $dbi = DBIx::Custom::SQLite->connect(database  => 'dbname');
    
    # Connect to memory database
    my $dbi = DBIx::Custom::SQLite->connect_memory;
    
    # Get last insert row id
    my $id = $dbi->last_insert_rowid;
    
=head1 ATTRIBUTES

This class is L<DBIx::Custom> subclass.
You can use all attributes of L<DBIx::Custom>.

=head2 C<database>

    my $database = $dbi->database;
    $dbi         = $dbi->database('your_database');

Database name.
This is used for connect().

=head1 METHODS

This class is L<DBIx::Custom> subclass.
You can use all methods of L<DBIx::Custom>.

=head2 C<connect (overridden)>
    
    $dbi = DBIx::Custom::SQLite->connect(
        data_source  => "dbi:SQLite:dbname=your_db"
    );
    
    $dbi = DBIx::Custom::SQLite->connect(database  => 'your_db');

Connect to database.
You can also specify database name, instead of data source.

=head2 C<connect_memory>

    $dbi->connect_memory;

Connect to memory database.

=head2 C<last_insert_rowid>

    $last_insert_rowid = $dbi->last_insert_rowid;

Get last insert row id.
This is equal to SQLite last_insert_rowid() function.

=cut
