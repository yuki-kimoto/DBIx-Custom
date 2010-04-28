use Test::More 'no_plan';
use strict;
use warnings;

use DBIx::Custom;
use DBIx::Custom::SQL::Template;

# Function for test name
my $test;
sub test {
    $test = shift;
}

# Variables for test
our $SQL_TMPL = {
    0 => DBIx::Custom::SQL::Template->new->tag_start(0),
    1 => DBIx::Custom::SQL::Template->new->tag_start(1),
    2 => DBIx::Custom::SQL::Template->new->tag_start(2)
};
my $dbi;


test 'Constructor';
$dbi = DBIx::Custom->new(
    user => 'a',
    database => 'a',
    password => 'b',
    data_source => 'c',
    options => {d => 1, e => 2},
    filters => {
        f => 3,
    },
    bind_filter => 'f',
    fetch_filter => 'g',
    result_class => 'g',
    sql_tmpl => $SQL_TMPL->{0},
);
is_deeply($dbi,{user => 'a', database => 'a', password => 'b', data_source => 'c', 
                options => {d => 1, e => 2}, filters => {f => 3}, bind_filter => 'f',
                fetch_filter => 'g', result_class => 'g',
                sql_tmpl => $SQL_TMPL->{0}}, $test);
isa_ok($dbi, 'DBIx::Custom');


test 'Sub class constructor';
{
    package DBIx::Custom::T1;
    use base 'DBIx::Custom';
    
    __PACKAGE__
      ->filters({f => 3})
      ->formats({f => 3})
    ;
}
$dbi = DBIx::Custom::T1->new(
    filters => {
        fo => 30,
    },
    formats => {
        fo => 30,
    },
);
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
is_deeply(scalar $dbi->formats, {fo => 30}, "$test : formats");

test 'Sub class constructor default';
$dbi = DBIx::Custom::T1->new;
is_deeply($dbi->filters, {f => 3}, "$test : filters");
is_deeply($dbi->formats, {f => 3}, "$test : formats");
isa_ok($dbi, 'DBIx::Custom::T1');


test 'Sub sub class constructor default';
{
    package DBIx::Custom::T1_2;
    use base 'DBIx::Custom::T1';
}
$dbi = DBIx::Custom::T1_2->new;
is_deeply(scalar $dbi->filters, {f => 3}, "$test : filters");
is_deeply(scalar $dbi->formats, {f => 3}, "$test : formats");
isa_ok($dbi, 'DBIx::Custom::T1_2');


test 'Customized sub class constructor default';
{
    package DBIx::Custom::T1_3;
    use base 'DBIx::Custom::T1';
    
    __PACKAGE__
      ->filters({fo => 30})
      ->formats({fo => 30})
    ;
}
$dbi = DBIx::Custom::T1_3->new;
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
is_deeply(scalar $dbi->formats, {fo => 30}, "$test : formats");
isa_ok($dbi, 'DBIx::Custom::T1_3');


test 'Customized sub class constructor';
$dbi = DBIx::Custom::T1_3->new(
    filters => {
        f => 3,
    },
    formats => {
        f => 3,
    },
);
is_deeply($dbi->filters, {f => 3}, "$test : filters");
is_deeply($dbi->formats, {f => 3}, "$test : formats");
isa_ok($dbi, 'DBIx::Custom');


test 'add_filters';
$dbi = DBIx::Custom->new;
$dbi->add_filter(a => sub {1});
is($dbi->filters->{a}->(), 1, $test);

test 'add_formats';
$dbi = DBIx::Custom->new;
$dbi->add_format(a => sub {1});
is($dbi->formats->{a}->(), 1, $test);

test 'filter_off';
$dbi = DBIx::Custom->new;
$dbi->bind_filter('a');
$dbi->fetch_filter('b');
$dbi->filter_off;
ok(!$dbi->bind_filter,  "$test : bind_filter  off");
ok(!$dbi->fetch_filter, "$test : fetch_filter off");

test 'Accessor';
$dbi = DBIx::Custom->new;
$dbi->options({opt1 => 1, opt2 => 2});
is_deeply(scalar $dbi->options, {opt1 => 1, opt2 => 2}, "$test : options");
