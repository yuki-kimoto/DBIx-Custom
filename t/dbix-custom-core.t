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
      ->user('a')
      ->database('a')
      ->password('b')
      ->data_source('c')
      ->options({d => 1, e => 2})
      ->filters({f => 3})
      ->formats({f => 3})
      ->bind_filter('f')
      ->fetch_filter('g')
      ->result_class('DBIx::Custom::Result')
      ->sql_tmpl($SQL_TMPL->{0})
    ;
}
$dbi = DBIx::Custom::T1->new(
    user => 'ao',
    database => 'ao',
    password => 'bo',
    data_source => 'co',
    options => {do => 10, eo => 20},
    filters => {
        fo => 30,
    },
    formats => {
        fo => 30,
    },
    bind_filter => 'fo',
    fetch_filter => 'go',
    result_class => 'ho',
    sql_tmpl => $SQL_TMPL->{0},
);
is($dbi->user, 'ao', "$test : user");
is($dbi->database, 'ao', "$test : database");
is($dbi->password, 'bo', "$test : passowr");
is($dbi->data_source, 'co', "$test : data_source");
is_deeply($dbi->options, {do => 10, eo => 20}, "$test : options");
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
is_deeply(scalar $dbi->formats, {fo => 30}, "$test : formats");
is($dbi->bind_filter, 'fo', "$test : bind_filter");
is($dbi->fetch_filter, 'go', "$test : fetch_filter");
is($dbi->result_class, 'ho', "$test : result_class");
is($dbi->sql_tmpl->tag_start, 0, "$test : sql_tmpl");
isa_ok($dbi, 'DBIx::Custom::T1');

test 'Sub class constructor default';
$dbi = DBIx::Custom::T1->new;
is($dbi->user, 'a', "$test : user");
is($dbi->database, 'a', "$test : database");
is($dbi->password, 'b', "$test : password");
is($dbi->data_source, 'c', "$test : data_source");
is_deeply($dbi->options, {d => 1, e => 2}, "$test : options");
is_deeply($dbi->filters, {f => 3}, "$test : filters");
is_deeply($dbi->formats, {f => 3}, "$test : formats");
is($dbi->bind_filter, 'f', "$test : bind_filter");
is($dbi->fetch_filter, 'g', "$test : fetch_filter");
is($dbi->result_class, 'DBIx::Custom::Result', "$test : result_class");
is($dbi->sql_tmpl->tag_start, 0, "$test : sql_tmpl");
isa_ok($dbi, 'DBIx::Custom::T1');


test 'Sub sub class constructor default';
{
    package DBIx::Custom::T1_2;
    use base 'DBIx::Custom::T1';
}
$dbi = DBIx::Custom::T1_2->new;
is($dbi->user, 'a', "$test : user");
is($dbi->database, 'a', "$test : database");
is($dbi->password, 'b', "$test : passowrd");
is($dbi->data_source, 'c', "$test : data_source");
is_deeply($dbi->options, {d => 1, e => 2}, "$test : options");
is_deeply(scalar $dbi->filters, {f => 3}, "$test : filters");
is_deeply(scalar $dbi->formats, {f => 3}, "$test : formats");
is($dbi->bind_filter, 'f', "$test : bind_filter");
is($dbi->fetch_filter, 'g', "$test : fetch_filter");
is($dbi->result_class, 'DBIx::Custom::Result', "$test : result_class");
is($dbi->sql_tmpl->tag_start, 0, "$test sql_tmpl");
isa_ok($dbi, 'DBIx::Custom::T1_2');


test 'Customized sub class constructor default';
{
    package DBIx::Custom::T1_3;
    use base 'DBIx::Custom::T1';
    
    __PACKAGE__
      ->user('ao')
      ->database('ao')
      ->password('bo')
      ->data_source('co')
      ->options({do => 10, eo => 20})
      ->filters({fo => 30})
      ->formats({fo => 30})
      ->bind_filter('fo')
      ->fetch_filter('go')
      ->result_class('ho')
      ->sql_tmpl($SQL_TMPL->{1})
    ;
}
$dbi = DBIx::Custom::T1_3->new;
is($dbi->user, 'ao', "$test : user");
is($dbi->database, 'ao', "$test : database");
is($dbi->password, 'bo', "$test : password");
is($dbi->data_source, 'co', "$test : data_source");
is_deeply($dbi->options, {do => 10, eo => 20}, "$test : options");
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
is_deeply(scalar $dbi->formats, {fo => 30}, "$test : formats");
is($dbi->bind_filter, 'fo', "$test : bind_filter");
is($dbi->fetch_filter, 'go', "$test : fetch_filter");
is($dbi->result_class, 'ho', "$test : result_class");
is($dbi->sql_tmpl->tag_start, 1, "$test : sql_tmpl");
isa_ok($dbi, 'DBIx::Custom::T1_3');


test 'Customized sub class constructor';
$dbi = DBIx::Custom::T1_3->new(
    user => 'a',
    database => 'a',
    password => 'b',
    data_source => 'c',
    options => {d => 1, e => 2},
    filters => {
        f => 3,
    },
    formats => {
        f => 3,
    },
    bind_filter => 'f',
    fetch_filter => 'g',
    result_class => 'h',
    sql_tmpl => $SQL_TMPL->{2},
);
is($dbi->user, 'a', "$test : user");
is($dbi->database, 'a', "$test : database");
is($dbi->password, 'b', "$test : password");
is($dbi->data_source, 'c', "$test : data_source");
is_deeply($dbi->options, {d => 1, e => 2}, "$test : options");
is_deeply($dbi->filters, {f => 3}, "$test : filters");
is_deeply($dbi->formats, {f => 3}, "$test : formats");
is($dbi->bind_filter, 'f', "$test : bind_filter");
is($dbi->fetch_filter, 'g', "$test : fetch_filter");
is($dbi->result_class, 'h', "$test : result_class");
is($dbi->sql_tmpl->tag_start, 2, "$test : sql_tmpl");
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

$dbi->no_bind_filters(['a', 'b']);
is_deeply(scalar $dbi->no_bind_filters, ['a', 'b'], "$test: no_bind_filters");

$dbi->no_fetch_filters(['a', 'b']);
is_deeply(scalar $dbi->no_fetch_filters, ['a', 'b'], "$test: no_fetch_filters");
