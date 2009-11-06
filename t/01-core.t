use Test::More 'no_plan';
use strict;
use warnings;

use DBI::Custom;
use DBI::Custom::SQL::Template;

# Function for test name
my $test;
sub test {
    $test = shift;
}

# Variables for test
our $SQL_TMPL = {
    0 => DBI::Custom::SQL::Template->new->tag_start(0),
    1 => DBI::Custom::SQL::Template->new->tag_start(1),
    2 => DBI::Custom::SQL::Template->new->tag_start(2)
};
my $dbi;


test 'Constructor';
$dbi = DBI::Custom->new(
    user => 'a',
    database => 'a',
    password => 'b',
    data_source => 'c',
    dbi_options => {d => 1, e => 2},
    filters => {
        f => 3,
    },
    bind_filter => 'f',
    fetch_filter => 'g',
    result_class => 'g',
    sql_template => $SQL_TMPL->{0},
);
is_deeply($dbi,{user => 'a', database => 'a', password => 'b', data_source => 'c', 
                dbi_options => {d => 1, e => 2}, filters => {f => 3}, bind_filter => 'f',
                fetch_filter => 'g', result_class => 'g',
                sql_template => $SQL_TMPL->{0}}, $test);
isa_ok($dbi, 'DBI::Custom');


test 'Sub class constructor';
{
    package DBI::Custom::T1;
    use base 'DBI::Custom';
    
    __PACKAGE__
      ->user('a')
      ->database('a')
      ->password('b')
      ->data_source('c')
      ->dbi_options({d => 1, e => 2})
      ->filters(
          f => 3
      )
      ->formats(
          f => 3
      )
      ->bind_filter('f')
      ->fetch_filter('g')
      ->result_class('DBI::Custom::Result')
      ->sql_template($SQL_TMPL->{0})
    ;
}
$dbi = DBI::Custom::T1->new(
    user => 'ao',
    database => 'ao',
    password => 'bo',
    data_source => 'co',
    dbi_options => {do => 10, eo => 20},
    filters => {
        fo => 30,
    },
    formats => {
        fo => 30,
    },
    bind_filter => 'fo',
    fetch_filter => 'go',
    result_class => 'ho',
    sql_template => $SQL_TMPL->{0},
);
is($dbi->user, 'ao', "$test : user");
is($dbi->database, 'ao', "$test : database");
is($dbi->password, 'bo', "$test : passowr");
is($dbi->data_source, 'co', "$test : data_source");
is_deeply($dbi->dbi_options, {do => 10, eo => 20}, "$test : dbi_options");
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
is_deeply(scalar $dbi->formats, {fo => 30}, "$test : formats");
is($dbi->bind_filter, 'fo', "$test : bind_filter");
is($dbi->fetch_filter, 'go', "$test : fetch_filter");
is($dbi->result_class, 'ho', "$test : result_class");
is($dbi->sql_template->tag_start, 0, "$test : sql_template");
isa_ok($dbi, 'DBI::Custom::T1');

test 'Sub class constructor default';
$dbi = DBI::Custom::T1->new;
is($dbi->user, 'a', "$test : user");
is($dbi->database, 'a', "$test : database");
is($dbi->password, 'b', "$test : password");
is($dbi->data_source, 'c', "$test : data_source");
is_deeply($dbi->dbi_options, {d => 1, e => 2}, "$test : dbi_options");
is_deeply({$dbi->filters}, {f => 3}, "$test : filters");
is_deeply({$dbi->formats}, {f => 3}, "$test : formats");
is($dbi->bind_filter, 'f', "$test : bind_filter");
is($dbi->fetch_filter, 'g', "$test : fetch_filter");
is($dbi->result_class, 'DBI::Custom::Result', "$test : result_class");
is($dbi->sql_template->tag_start, 0, "$test : sql_template");
isa_ok($dbi, 'DBI::Custom::T1');


test 'Sub sub class constructor default';
{
    package DBI::Custom::T1_2;
    use base 'DBI::Custom::T1';
}
$dbi = DBI::Custom::T1_2->new;
is($dbi->user, 'a', "$test : user");
is($dbi->database, 'a', "$test : database");
is($dbi->password, 'b', "$test : passowrd");
is($dbi->data_source, 'c', "$test : data_source");
is_deeply($dbi->dbi_options, {d => 1, e => 2}, "$test : dbi_options");
is_deeply(scalar $dbi->filters, {f => 3}, "$test : filters");
is_deeply(scalar $dbi->formats, {f => 3}, "$test : formats");
is($dbi->bind_filter, 'f', "$test : bind_filter");
is($dbi->fetch_filter, 'g', "$test : fetch_filter");
is($dbi->result_class, 'DBI::Custom::Result', "$test : result_class");
is($dbi->sql_template->tag_start, 0, "$test sql_template");
isa_ok($dbi, 'DBI::Custom::T1_2');


test 'Customized sub class constructor default';
{
    package DBI::Custom::T1_3;
    use base 'DBI::Custom::T1';
    
    __PACKAGE__
      ->user('ao')
      ->database('ao')
      ->password('bo')
      ->data_source('co')
      ->dbi_options({do => 10, eo => 20})
      ->filters(
        fo => 30
      )
      ->formats(
        fo => 30
      )
      ->bind_filter('fo')
      ->fetch_filter('go')
      ->result_class('ho')
      ->sql_template($SQL_TMPL->{1})
    ;
}
$dbi = DBI::Custom::T1_3->new;
is($dbi->user, 'ao', "$test : user");
is($dbi->database, 'ao', "$test : database");
is($dbi->password, 'bo', "$test : password");
is($dbi->data_source, 'co', "$test : data_source");
is_deeply($dbi->dbi_options, {do => 10, eo => 20}, "$test : dbi_options");
is_deeply(scalar $dbi->filters, {fo => 30}, "$test : filters");
is_deeply(scalar $dbi->formats, {fo => 30}, "$test : formats");
is($dbi->bind_filter, 'fo', "$test : bind_filter");
is($dbi->fetch_filter, 'go', "$test : fetch_filter");
is($dbi->result_class, 'ho', "$test : result_class");
is($dbi->sql_template->tag_start, 1, "$test : sql_template");
isa_ok($dbi, 'DBI::Custom::T1_3');


test 'Customized sub class constructor';
$dbi = DBI::Custom::T1_3->new(
    user => 'a',
    database => 'a',
    password => 'b',
    data_source => 'c',
    dbi_options => {d => 1, e => 2},
    filters => {
        f => 3,
    },
    formats => {
        f => 3,
    },
    bind_filter => 'f',
    fetch_filter => 'g',
    result_class => 'h',
    sql_template => $SQL_TMPL->{2},
);
is($dbi->user, 'a', "$test : user");
is($dbi->database, 'a', "$test : database");
is($dbi->password, 'b', "$test : password");
is($dbi->data_source, 'c', "$test : data_source");
is_deeply($dbi->dbi_options, {d => 1, e => 2}, "$test : dbi_options");
is_deeply({$dbi->filters}, {f => 3}, "$test : filters");
is_deeply({$dbi->formats}, {f => 3}, "$test : formats");
is($dbi->bind_filter, 'f', "$test : bind_filter");
is($dbi->fetch_filter, 'g', "$test : fetch_filter");
is($dbi->result_class, 'h', "$test : result_class");
is($dbi->sql_template->tag_start, 2, "$test : sql_template");
isa_ok($dbi, 'DBI::Custom');


test 'add_filters';
$dbi = DBI::Custom->new;
$dbi->add_filter(a => sub {1});
is($dbi->filters->{a}->(), 1, $test);

test 'add_formats';
$dbi = DBI::Custom->new;
$dbi->add_format(a => sub {1});
is($dbi->formats->{a}->(), 1, $test);

