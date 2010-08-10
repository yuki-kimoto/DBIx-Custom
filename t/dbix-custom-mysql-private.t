use Test::More;
use strict;
use warnings;

# user password database
our ($USER, $PASSWORD, $DATABASE) = connect_info();

plan skip_all => 'private MySQL test' unless $USER;

plan 'no_plan';

# Function for test name
my $test;
sub test {
    $test = shift;
}


# Functions for tests
sub connect_info {
    my $file = 'password.tmp';
    open my $fh, '<', $file
      or return;
    
    my ($user, $password, $database) = split(/\s/, (<$fh>)[0]);
    
    close $fh;
    
    return ($user, $password, $database);
}


# Varialbes for tests
my $dbi;
my $dbname;

use DBIx::Custom::MySQL;

test 'connect';
$dbi = DBIx::Custom::MySQL->new(user => $USER, password => $PASSWORD,
                    database => $DATABASE, host => 'localhost', port => '10000');
$dbi->connect;
like($dbi->data_source, qr/dbi:mysql:database=.*;host=localhost;port=10000;/, "$test : created data source");
is(ref $dbi->dbh, 'DBI::db', $test);

test 'attributes';
$dbi = DBIx::Custom::MySQL->new;
$dbi->host('a');
is($dbi->host, 'a', "$test: host");
$dbi->port('b');
is($dbi->port, 'b', "$test: port");
