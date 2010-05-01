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
    columns        => 'b',
    filter      => 'c',
    sth              => 'e',
    fetch_filter     => 'f',
);

is($query->sql, 'a', "$test : sql");
is($query->columns, 'b', "$test : columns ");
is($query->filter, 'c', "$test : filter");
is($query->sth, 'e', "$test : sth");

