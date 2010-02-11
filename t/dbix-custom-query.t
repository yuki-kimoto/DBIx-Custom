use Test::More 'no_plan';

use strict;
use warnings;
use DBIx::Custom::Query;

# Function for test name
my $test;
sub test{
    $test = shift;
}

# Variables for test
my $query;

test 'Accessors';
$query = DBIx::Custom::Query->new(
    sql              => 'a',
    key_infos        => 'b',
    bind_filter      => 'c',
    sth              => 'e',
    fetch_filter     => 'f',
);

is($query->sql, 'a', "$test : sql");
is($query->key_infos, 'b', "$test : key_infos ");
is($query->bind_filter, 'c', "$test : bind_filter");
is($query->sth, 'e', "$test : sth");

