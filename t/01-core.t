use Test::More 'no_plan';
use strict;
use warnings;

use DBI::Custom;
use Scalar::Util qw/blessed/;

my $sql_tmpl1 = DBI::Custom::SQL::Template->new->upper_case(0);
my $sql_tmpl2 = DBI::Custom::SQL::Template->new->upper_case(1);
my $sql_tmpl3 = DBI::Custom::SQL::Template->new->upper_case(2);

{
    my $dbi = DBI::Custom->new(
        connect_info => {
          user => 'a',
          password => 'b',
          data_source => 'c',
          options => {d => 1, e => 2}
        },
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        dbh => 'e',
        result_class => 'g',
        sql_template => $sql_tmpl1,
        valid_connect_info => {i => 1}
    );
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', 
                    options => {d => 1, e => 2}}, filters => {f => 3}, bind_filter => 'f',
                    fetch_filter => 'g', dbh => 'e', result_class => 'g',
                    sql_template => $sql_tmpl1, valid_connect_info => {i => 1}}, 'new');
    
    isa_ok($dbi, 'DBI::Custom');
}


{
    package DBI::Custom::T1;
    use base 'DBI::Custom';
    
    my $prototype = __PACKAGE__->prototype;
    
    $prototype
      ->connect_info(
          user => 'a',
          password => 'b',
          data_source => 'c',
          options => {d => 1, e => 2}
      )
      ->filters(
          f => 3
      )
      ->bind_filter('f')
      ->fetch_filter('g')
      ->dbh('e')
      ->result_class('DBI::Custom::Result')
      ->sql_template($sql_tmpl1)
      ->valid_connect_info({p => 1})
    ;
}
{
    my $dbi = DBI::Custom::T1->new(
        connect_info => {
            user => 'ao',
            password => 'bo',
            data_source => 'co',
            options => {do => 10, eo => 20}
        },
        filters => {
            fo => 30,
        },
        bind_filter => 'fo',
        fetch_filter => 'go',
        result_class => 'ho',
        sql_template => $sql_tmpl1,
        valid_connect_info => {io => 1}
    );
    my $sql_tmpl = delete $dbi->{sql_template};
    is($sql_tmpl->upper_case, 0);
    is_deeply($dbi,{connect_info => {user => 'ao', password => 'bo', data_source => 'co', options => {do => 10, eo => 20}}
                    ,filters => {fo => 30}, bind_filter => 'fo', fetch_filter => 'go', result_class => 'ho',
                    ,valid_connect_info => {io => 1}
                    }, 'new arguments');
    
    isa_ok($dbi, 'DBI::Custom::T1');
}

{
    my $dbi = DBI::Custom::T1->new;
    
    my $sql_tmpl = delete $dbi->{sql_template};
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}},
                    filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g', result_class => 'DBI::Custom::Result',
                    valid_connect_info => {p => 1}}, 'new custom class');
    
    isa_ok($dbi, 'DBI::Custom::T1');
    
}

{
    package DBI::Custom::T1_2;
    use base 'DBI::Custom::T1';
}

{
    my $dbi = DBI::Custom::T1_2->new;
    
    my $sql_tmpl = delete $dbi->{sql_template};
    is($sql_tmpl->upper_case, 0);
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}},
                    filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g', result_class => 'DBI::Custom::Result',
                    valid_connect_info => {p => 1}}, 'new custom class inherit');
    
    isa_ok($dbi, 'DBI::Custom::T1_2');
}

{
    package DBI::Custom::T1_3;
    use base 'DBI::Custom::T1';
    
    my $prototype = __PACKAGE__->prototype;
        
    $prototype
      ->connect_info(
        user => 'ao',
        password => 'bo',
        data_source => 'co',
        options => {do => 10, eo => 20}
      )
      ->filters(
        fo => 30
      )
      ->bind_filter('fo')
      ->fetch_filter('go')
      ->dbh('eo')
      ->result_class('ho')
      ->sql_template($sql_tmpl2)
      ->valid_connect_info({p => 3})
    ;
}

{
    my $dbi = DBI::Custom::T1_3->new;
    
    my $sql_tmpl = delete $dbi->{sql_template};
    is($sql_tmpl->upper_case, 1);
    is_deeply($dbi,{connect_info => {user => 'ao', password => 'bo', data_source => 'co', options => {do => 10, eo => 20}},
                    filters => {fo => 30}, bind_filter => 'fo', fetch_filter => 'go', result_class => 'ho',
                    valid_connect_info => {p => 3}}, 'new custom class');
    
    isa_ok($dbi, 'DBI::Custom::T1_3');
}

{
    my $dbi = DBI::Custom::T1_3->new(
        connect_info => {
            user => 'a',
            password => 'b',
            data_source => 'c',
            options => {d => 1, e => 2}
        },
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        dbh => 'e',
        result_class => 'h',
        sql_template => $sql_tmpl3,
        valid_connect_info => {p => 4}
    );
    
    my $sql_tmpl = delete $dbi->{sql_template};
    is($sql_tmpl->upper_case, 2);
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}},
                    filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g', dbh => 'e', result_class => 'h',
                    valid_connect_info => {p => 4}}, 'new');
    
    isa_ok($dbi, 'DBI::Custom');
}


{
    my $dbi = DBI::Custom->new(
        connect_info => {
            no_exist => 1,
        }
    );
    eval{$dbi->connect};
    
    like($@, qr/connect_info 'no_exist' is wrong name/, 'no exist');
}

{
    my $dbi = DBI::Custom->new;
    my $tmpl   = "select * from table where {= title};";
    my $values = {title => 'a'};
    my ($sql, @bind) = $dbi->create_sql($tmpl, $values);
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
    
    my ($sql, @bind) = $dbi->create_sql($tmpl, $values, $dbi->filters->{filter});
    
    is($sql, "select * from table where k1 = ? && k2 <> ? && k3 < ? && k4 > ? && k5 >= ? && k6 <= ? && k7 like ?;", 'sql template2');
    is_deeply(\@bind, ['A', 'b', 'c', 'd', 'e', 'f', 'g'], 'sql template bind2' );
}

{
    # Expand place holer upper case
    my $dbi = DBI::Custom->new;
    $dbi->sql_template->upper_case(1);
    my $tmpl   = "select * from table where {like k7}";
    my $values = {k7 => 'g'};
    
    my ($sql, @bind) = $dbi->create_sql($tmpl, $values);
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
        
    my ($sql, @bind) = $dbi->create_sql($tmpl, $values, $dbi->filters->{filter});
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
        
    my ($sql, @bind) = $dbi->create_sql($tmpl, $values, $dbi->filters->{filter});
    is($sql, "update table set k1 = ?, k2 = ?;");
    is_deeply(\@bind, ['A', 'b'], 'sql template bind' );
}

