package DBIx::Custom::MySQL;

use strict;
use warnings;

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

sub last_insert_id { shift->dbh->{mysql_insertid} }

1;

=head1 NAME

DBIx::Custom::MySQL - MySQL implementation

=head1 SYNOPSYS

    # Connect to database
    my $dbi = DBIx::Custom::MySQL->connect(user     => 'taro', 
                                           password => 'kliej&@K',
                                           database => 'your_database');
    
    # Get last insert id
    my $id = $dbi->last_insert_id;

=head1 ATTRIBUTES

This class is L<DBIx::Custom> subclass.
You can use all attributes of L<DBIx::Custom>

=head2 C<database>

    my $database = $dbi->database;
    $dbi         = $dbi->database('your_database');

Database name.
This is used for connect().

=head2 C<host>

    my $host = $dbi->host;
    $dbi     = $dbi->host('somehost.com');

Database host name.
You can also set IP address, instead of host name.
This is used for connect().

    $dbi->host('127.03.45.12');

=head2 C<port>

    my $port = $dbi->port;
    $dbi     = $dbi->port(1198);

Database port. This is used for connect().

=head1 METHODS

This class is L<DBIx::Custom> subclass.
You can use all methods of L<DBIx::Custom>.

=head2 C<connect (overridden)>

    $dbi = DBIx::Custom::MySQL->connect(
        data_source => "dbi:mysql:database=books;host=somehost;port=2000"
    );
    
    $dbi = DBIx::Custom::MySQL->connect(user     => 'taro', 
                                        password => 'kliej&@K',
                                        database => 'your_database',
                                        host     => 'somehost',
                                        port     => 2000);

Connect to database. You can also specify database, host, and port
(instead of data soruce).

=head2 C<last_insert_id>

    $last_insert_id = $dbi->last_insert_id;

Get last insert id.
This is equal to MySQL last_insert_id() function.

=cut
