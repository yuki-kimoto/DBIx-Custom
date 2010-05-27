package DBIx::Custom::MySQL;

use warnings;
use strict;

use base 'DBIx::Custom';

use Carp 'croak';

__PACKAGE__->attr([qw/database host port/]);

sub connect {
    my $proto = shift;
    
    # Create
    my $self = ref $proto ? $proto : $proto->new(@_);
    
    # Create data source
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

sub last_insert_id {
    my $self = shift;
    
    # Not connected
    croak "Not yet connected" unless $self->connected;
    
    # Get last insert id
    my $last_insert_id = $self->dbh->{mysql_insertid};
    
    return $last_insert_id;
}

1;

=head1 NAME

DBIx::Custom::MySQL - a MySQL implementation of DBIx::Custom

=head1 SYNOPSYS

    # Connect
    my $dbi = DBIx::Custom::MySQL->connect(user      => 'taro', 
                                           password => 'kliej&@K',
                                           database  => 'your_database');
    
    # Last insert id
    my $id = $dbi->last_insert_id;

=head1 ATTRIBUTES

This class is L<DBIx::Custom> subclass.
You can use all attributes of L<DBIx::Custom>

=head2 database

Database name

    $dbi      = $dbi->database('your_database');
    $database = $dbi->database;

=head2 host

Database host name.

    $dbi  = $dbi->host('somehost.com');
    $host = $dbi->host;

IP address can be set to host attribute.

    $dbi->host('127.03.45.12');

=head2 port

Database port.

    $dbi  = $dbi->port(1198);
    $port = $dbi->port;

=head1 METHODS

This class is L<DBIx::Custom> subclass.
You can use all methods of L<DBIx::Custom>.

=head2 connect - overridden

Connect to database.

    # Connect
    my $dbi = DBIx::Custom::MySQL->connect(user      => 'taro', 
                                           password => 'kliej&@K',
                                           database  => 'your_database');

=head2 last_insert_id

Last insert ID.

    $last_insert_id = $dbi->last_insert_id;

This is equal to MySQL last_insert_id() function.

    
=cut
