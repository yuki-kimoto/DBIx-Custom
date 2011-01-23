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

=head1 CAUTION

B<This module is deprecated now> because This module is less useful
than I expected. Please use DBIx::Custom instead.>

=head1 SYNOPSYS

    # Connect to the database
    my $dbi = DBIx::Custom::MySQL->connect(
        user     => 'taro', 
        password => 'kliej&@K',
        database => 'dbname'
    );
    
    # Get last insert id
    my $last_insert_id = $dbi->last_insert_id;

=head1 ATTRIBUTES

L<DBIx::Custom::MySQL> inherits all attributes from L<DBIx::Custom>
and implements the following new ones.

=head2 C<database>

    my $database = $dbi->database;
    $dbi         = $dbi->database('dbname');

Database name.
C<connect()> method use this value to connect the database
if C<data_source> is not specified.

=head2 C<host>

    my $host = $dbi->host;
    $dbi     = $dbi->host('somehost');

Host name or IP address.
C<connect()> method use this value to connect the database
if C<data_source> is not specified.

=head2 C<port>

    my $port = $dbi->port;
    $dbi     = $dbi->port(1198);

Port number.
C<connect()> method use this value to connect the database
if C<data_source> is not specified.

=head1 METHODS

L<DBIx::Custom::MySQL> inherits all methods from L<DBIx::Custom>
and implements the following new ones.

=head2 C<connect>

    my $dbi = DBIx::Custom::MySQL->connect(
         user     => 'taro', 
         password => 'kliej&@K',
         database => 'dbname',
         host     => 'somehost',
         port     => 2000
    );

Create a new L<DBIx::Custom::MySQL> object and connect to the database.
This method overrides C<DBIx::Custom::connect()> method.
You can specify all attributes of L<DBIx::Custom>
and L<DBIx::Custom::MySQL>, such as C<database>, C<host>, C<port>.

=cut
