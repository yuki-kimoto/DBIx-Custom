use Test::More 'no_plan';
use strict;
use warnings;

use DBI::Custom;
use Scalar::Util qw/blessed/;
use DBI::Custom::SQL::Template;

my $sql_tmpl1 = DBI::Custom::SQL::Template->new->tag_start(0);
my $sql_tmpl2 = DBI::Custom::SQL::Template->new->tag_start(1);
my $sql_tmpl3 = DBI::Custom::SQL::Template->new->tag_start(2);

{
    my $dbi = DBI::Custom->new(
        user => 'a',
        password => 'b',
        data_source => 'c',
        dbi_options => {d => 1, e => 2},
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        result_class => 'g',
        sql_template => $sql_tmpl1,
    );
    is_deeply($dbi,{user => 'a', password => 'b', data_source => 'c', 
                    dbi_options => {d => 1, e => 2}, filters => {f => 3}, bind_filter => 'f',
                    fetch_filter => 'g', result_class => 'g',
                    sql_template => $sql_tmpl1}, 'new');
    
    isa_ok($dbi, 'DBI::Custom');
}


{
    package DBI::Custom::T1;
    use base 'DBI::Custom';
    
    my $class = __PACKAGE__;
    
    $class
      ->user('a')
      ->password('b')
      ->data_source('c')
      ->dbi_options({d => 1, e => 2})
      ->filters(
          f => 3
      )
      ->bind_filter('f')
      ->fetch_filter('g')
      ->result_class('DBI::Custom::Result')
      ->sql_template($sql_tmpl1)
    ;
}
{
    my $dbi = DBI::Custom::T1->new(
        user => 'ao',
        password => 'bo',
        data_source => 'co',
        dbi_options => {do => 10, eo => 20},
        filters => {
            fo => 30,
        },
        bind_filter => 'fo',
        fetch_filter => 'go',
        result_class => 'ho',
        sql_template => $sql_tmpl1,
    );
    my $sql_tmpl = delete $dbi->{sql_template};
    is($sql_tmpl->tag_start, 0);
    is_deeply($dbi,{ user => 'ao', password => 'bo', data_source => 'co', dbi_options => {do => 10, eo => 20},
                    ,filters => {fo => 30}, bind_filter => 'fo', fetch_filter => 'go', result_class => 'ho',
                    }, 'new arguments');
    
    isa_ok($dbi, 'DBI::Custom::T1');
}

{
    my $dbi = DBI::Custom::T1->new;
    
    is($dbi->user, 'a');
    is($dbi->password, 'b');
    is($dbi->data_source, 'c');
    is_deeply($dbi->dbi_options, {d => 1, e => 2});
    is_deeply({$dbi->filters}, {f => 3});
    is($dbi->bind_filter, 'f');
    is($dbi->fetch_filter, 'g');
    is($dbi->result_class, 'DBI::Custom::Result');
    is($dbi->sql_template->tag_start, 0);
    isa_ok($dbi, 'DBI::Custom::T1');
    
}

{
    package DBI::Custom::T1_2;
    use base 'DBI::Custom::T1';
}

{
    my $dbi = DBI::Custom::T1_2->new;
    
    is($dbi->user, 'a');
    is($dbi->password, 'b');
    is($dbi->data_source, 'c');
    is_deeply($dbi->dbi_options, {d => 1, e => 2});
    is_deeply(scalar $dbi->filters, {f => 3});
    is($dbi->bind_filter, 'f');
    is($dbi->fetch_filter, 'g');
    is($dbi->result_class, 'DBI::Custom::Result');
    is($dbi->sql_template->tag_start, 0);
    
    isa_ok($dbi, 'DBI::Custom::T1_2');
}

{
    package DBI::Custom::T1_3;
    use base 'DBI::Custom::T1';
    
    my $class = __PACKAGE__;
        
    $class
      ->user('ao')
      ->password('bo')
      ->data_source('co')
      ->dbi_options({do => 10, eo => 20})
      ->filters(
        fo => 30
      )
      ->bind_filter('fo')
      ->fetch_filter('go')
      ->result_class('ho')
      ->sql_template($sql_tmpl2)
    ;
}

{
    my $dbi = DBI::Custom::T1_3->new;
    
    is($dbi->user, 'ao');
    is($dbi->password, 'bo');
    is($dbi->data_source, 'co');
    is_deeply($dbi->dbi_options, {do => 10, eo => 20});
    is_deeply(scalar $dbi->filters, {fo => 30});
    is($dbi->bind_filter, 'fo');
    is($dbi->fetch_filter, 'go');
    is($dbi->result_class, 'ho');
    is($dbi->sql_template->tag_start, 1);
    
    isa_ok($dbi, 'DBI::Custom::T1_3');
}

{
    my $dbi = DBI::Custom::T1_3->new(
        user => 'a',
        password => 'b',
        data_source => 'c',
        dbi_options => {d => 1, e => 2},
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        result_class => 'h',
        sql_template => $sql_tmpl3,
    );
    
    is($dbi->user, 'a');
    is($dbi->password, 'b');
    is($dbi->data_source, 'c');
    is_deeply($dbi->dbi_options, {d => 1, e => 2});
    is_deeply({$dbi->filters}, {f => 3});
    is($dbi->bind_filter, 'f');
    is($dbi->fetch_filter, 'g');
    is($dbi->result_class, 'h');
    is($dbi->sql_template->tag_start, 2);
    
    isa_ok($dbi, 'DBI::Custom');
}

