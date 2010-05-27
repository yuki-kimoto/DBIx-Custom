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


# Constat variables for tests
my $CLASS = 'DBIx::Custom::MySQL';

# Varialbes for tests
my $dbi;

use DBIx::Custom::MySQL;

test 'connect';
$dbi = $CLASS->new(user => $USER, password => $PASSWORD,
                    database => $DATABASE);
$dbi->connect;
is(ref $dbi->dbh, 'DBI::db', $test);

test 'attributes';
$dbi = DBIx::Custom::MySQL->new;
$dbi->host('a');
is($dbi->host, 'a', "$test: host");
$dbi->port('b');
is($dbi->port, 'b', "$test: port");
