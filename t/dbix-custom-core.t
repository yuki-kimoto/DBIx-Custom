use Test::More 'no_plan';
use strict;
use warnings;

use DBIx::Custom;
use DBIx::Custom::QueryBuilder;

# Function for test name
my $test;
sub test {
    $test = shift;
}

# Variables for test
my $dbi;
my $query_builder;

test 'Constructor';
$query_builder = DBIx::Custom::QueryBuilder->new;
$dbi = DBIx::Custom->new(
    user => 'a',
    database => 'a',
    password => 'b',
    data_source => 'c',
    filters => {
        f => 3,
    },
    default_bind_filter => 'f',
    default_fetch_filter => 'g',
    result_class => 'g',
    query_builder => $query_builder,
);
is_deeply($dbi,{user => 'a', database => 'a', password => 'b', data_source => 'c', 
                filters => {f => 3}, default_bind_filter => 'f',
                default_fetch_filter => 'g', result_class => 'g',
                query_builder => $query_builder}, $test);
isa_ok($dbi, 'DBIx::Custom');


test 'Sub class constructor';
{
    package DBIx::Custom::T1;
    use base 'DBIx::Custom';
    
    __PACKAGE__
      ->filters({f => 3})
    ;
}
$dbi = DBIx::Custom::T1->new(
    filters => {
        fo => 30,
    },
);
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");

test 'Sub class constructor default';
$dbi = DBIx::Custom::T1->new;
is_deeply($dbi->filters, {f => 3}, "$test : filters");
isa_ok($dbi, 'DBIx::Custom::T1');


test 'Sub sub class constructor default';
{
    package DBIx::Custom::T1_2;
    use base 'DBIx::Custom::T1';
}
$dbi = DBIx::Custom::T1_2->new;
is_deeply(scalar $dbi->filters, {f => 3}, "$test : filters");
isa_ok($dbi, 'DBIx::Custom::T1_2');


test 'Customized sub class constructor default';
{
    package DBIx::Custom::T1_3;
    use base 'DBIx::Custom::T1';
    
    __PACKAGE__
      ->filters({fo => 30})
    ;
}
$dbi = DBIx::Custom::T1_3->new;
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
isa_ok($dbi, 'DBIx::Custom::T1_3');


test 'Customized sub class constructor';
$dbi = DBIx::Custom::T1_3->new(
    filters => {
        f => 3,
    },
);
is_deeply($dbi->filters, {f => 3}, "$test : filters");
isa_ok($dbi, 'DBIx::Custom');


test 'register_filters';
$dbi = DBIx::Custom->new;
$dbi->register_filter(a => sub {1});
is($dbi->filters->{a}->(), 1, $test);
$dbi->register_filter({b => sub {2}});
is($dbi->filters->{b}->(), 2, $test);


