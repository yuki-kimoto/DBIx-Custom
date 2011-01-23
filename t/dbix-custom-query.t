use Test::More 'no_plan';

use strict;
use warnings;
use DBIx::Custom::Query;

# Function for test name
sub test{ "# $_[0]\n" }

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

is($query->sql, 'a', "sql");
is($query->columns, 'b', "columns ");
is($query->filter, 'c', "filter");
is($query->sth, 'e', "sth");

