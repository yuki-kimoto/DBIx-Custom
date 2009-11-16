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
    no_bind_filters  => [qw/d e/],
    sth              => 'e',
    fetch_filter     => 'f',
    no_fetch_filters => [qw/g h/],
);

is($query->sql, 'a', "$test : sql");
is($query->key_infos, 'b', "$test : key_infos ");
is($query->bind_filter, 'c', "$test : bind_filter");
is_deeply(scalar $query->no_bind_filters, [qw/d e/], "$test : no_bind_filters");
is_deeply(scalar $query->_no_bind_filters_map, {d => 1, e => 1}, "$test : _no_bind_filters_map");
is_deeply(scalar $query->no_fetch_filters, [qw/g h/], "$test : no_fetch_filters");
is($query->sth, 'e', "$test : sth");

$query->no_bind_filters(undef);
is_deeply(scalar $query->_no_bind_filters_map, {}, "$test _no_bind_filters_map undef value");

