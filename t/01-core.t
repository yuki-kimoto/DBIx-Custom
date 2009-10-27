use Test::More 'no_plan';
use strict;
use warnings;

use DBI::Custom;
use Scalar::Util qw/blessed/;
use DBI::Custom::SQL::Template;

my $sql_tmpl1 = DBI::Custom::SQL::Template->new->upper_case(0);
my $sql_tmpl2 = DBI::Custom::SQL::Template->new->upper_case(1);
my $sql_tmpl3 = DBI::Custom::SQL::Template->new->upper_case(2);

{
    my $dbi = DBI::Custom->new(
        user => 'a',
        password => 'b',
        data_source => 'c',
        dbi_option => {d => 1, e => 2},
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        result_class => 'g',
        sql_template => $sql_tmpl1,
    );
    is_deeply($dbi,{user => 'a', password => 'b', data_source => 'c', 
                    dbi_option => {d => 1, e => 2}, filters => {f => 3}, bind_filter => 'f',
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
      ->dbi_option({d => 1, e => 2})
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
        dbi_option => {do => 10, eo => 20},
        filters => {
            fo => 30,
        },
        bind_filter => 'fo',
        fetch_filter => 'go',
        result_class => 'ho',
        sql_template => $sql_tmpl1,
    );
    my $sql_tmpl = delete $dbi->{sql_template};
    is($sql_tmpl->upper_case, 0);
    is_deeply($dbi,{ user => 'ao', password => 'bo', data_source => 'co', dbi_option => {do => 10, eo => 20},
                    ,filters => {fo => 30}, bind_filter => 'fo', fetch_filter => 'go', result_class => 'ho',
                    }, 'new arguments');
    
    isa_ok($dbi, 'DBI::Custom::T1');
}

{
    my $dbi = DBI::Custom::T1->new;
    
    is($dbi->user, 'a');
    is($dbi->password, 'b');
    is($dbi->data_source, 'c');
    is_deeply($dbi->dbi_option, {d => 1, e => 2});
    is_deeply({$dbi->filters}, {f => 3});
    is($dbi->bind_filter, 'f');
    is($dbi->fetch_filter, 'g');
    is($dbi->result_class, 'DBI::Custom::Result');
    is($dbi->sql_template->upper_case, 0);
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
    is_deeply($dbi->dbi_option, {d => 1, e => 2});
    is_deeply(scalar $dbi->filters, {f => 3});
    is($dbi->bind_filter, 'f');
    is($dbi->fetch_filter, 'g');
    is($dbi->result_class, 'DBI::Custom::Result');
    is($dbi->sql_template->upper_case, 0);
    
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
      ->dbi_option({do => 10, eo => 20})
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
    is_deeply($dbi->dbi_option, {do => 10, eo => 20});
    is_deeply(scalar $dbi->filters, {fo => 30});
    is($dbi->bind_filter, 'fo');
    is($dbi->fetch_filter, 'go');
    is($dbi->result_class, 'ho');
    is($dbi->sql_template->upper_case, 1);
    
    isa_ok($dbi, 'DBI::Custom::T1_3');
}

{
    my $dbi = DBI::Custom::T1_3->new(
        user => 'a',
        password => 'b',
        data_source => 'c',
        dbi_option => {d => 1, e => 2},
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
    is_deeply($dbi->dbi_option, {d => 1, e => 2});
    is_deeply({$dbi->filters}, {f => 3});
    is($dbi->bind_filter, 'f');
    is($dbi->fetch_filter, 'g');
    is($dbi->result_class, 'h');
    is($dbi->sql_template->upper_case, 2);
    
    isa_ok($dbi, 'DBI::Custom');
}

{
    my $dbi = DBI::Custom->new;
    my $tmpl   = "select * from table where {= title};";
    my $values = {title => 'a'};
    my ($sql, @bind) = $dbi->_create_sql($tmpl, $values);
    is($sql, "select * from table where title = ?;", 'sql template');
    is_deeply(\@bind, ['a'], 'sql template bind' );
}

{
    # Expand place holer
    my $dbi = DBI::Custom->new;
    my $tmpl   = "select * from table where {= k1} && {<> k2} && {< k3} && {> k4} && {>= k5} && {<= k6} && {like k7}";
    my $values = {k1 => 'a', k2 => 'b', k3 => 'c', k4 => 'd', k5 => 'e', k6 => 'f', k7 => 'g'};
    
    $dbi->filters(filter => sub {
        my ($key, $value) = @_;
        if ($key eq 'k1' && $value eq 'a') {
            return uc $value;
        }
        return $value;
    });
    
    my ($sql, @bind) = $dbi->_create_sql($tmpl, $values, $dbi->filters->{filter});
    
    is($sql, "select * from table where k1 = ? && k2 <> ? && k3 < ? && k4 > ? && k5 >= ? && k6 <= ? && k7 like ?;", 'sql template2');
    is_deeply(\@bind, ['A', 'b', 'c', 'd', 'e', 'f', 'g'], 'sql template bind2' );
}

{
    # Expand place holer upper case
    my $dbi = DBI::Custom->new;
    $dbi->sql_template->upper_case(1);
    my $tmpl   = "select * from table where {like k7}";
    my $values = {k7 => 'g'};
    
    my ($sql, @bind) = $dbi->_create_sql($tmpl, $values);
    is($sql, "select * from table where k7 LIKE ?;", 'sql template2');
    is_deeply(\@bind, ['g'], 'sql template bind2' );
}


{
    # Insert values
    my $dbi = DBI::Custom->new;
    my $tmpl   = "insert into table {insert_values}";
    my $values = {insert_values => {k1 => 'a', k2 => 'b'}};
    
    $dbi->filters(filter => sub {
        my ($key, $value) = @_;
        if ($key eq 'k1' && $value eq 'a') {
            return uc $value;
        }
        return $value;
    });
        
    my ($sql, @bind) = $dbi->_create_sql($tmpl, $values, $dbi->filters->{filter});
    is($sql, "insert into table (k1, k2) values (?, ?);");
    is_deeply(\@bind, ['A', 'b'], 'sql template bind' );
}

{
    # Update set
    my $dbi = DBI::Custom->new;
    my $tmpl   = "update table {update_set}";
    my $values = {update_set => {k1 => 'a', k2 => 'b'}};

    $dbi->filters(filter => sub {
        my ($key, $value) = @_;
        if ($key eq 'k1' && $value eq 'a') {
            return uc $value;
        }
        return $value;
    });
        
    my ($sql, @bind) = $dbi->_create_sql($tmpl, $values, $dbi->filters->{filter});
    is($sql, "update table set k1 = ?, k2 = ?;");
    is_deeply(\@bind, ['A', 'b'], 'sql template bind' );
}

