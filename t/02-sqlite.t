use Test::More;
use strict;
use warnings;

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1 }
        or plan skip_all => 'DBD::SQLite >= 1.00 required';

    plan 'no_plan';
    use_ok('DBI::Custom');
}

my $dbi = DBI::Custom->new(
   connect_info => {data_source => 'dbi:SQLite:dbname=:memory:'}
);

$dbi->query_raw_sql("create table t1 (k1 char(10), k2 char(10))");

{
    $dbi->query("insert into t1 {insert_values}",{insert_values => {k1 => 1, k2 => 2}});
    
    $dbi->fetch_filter(sub {
        my ($key, $value) = @_;
        if ($key eq 'k1' && $value == 1 ) {
            return $value * 3;
        }
        return $value;
    });
    
    my $result = $dbi->query("select k1, k2 from t1");
    
    my $row = $result->fetchrow_arrayref;
    my @values = @$row;
    $result->finish;
    
    is_deeply(\@values, [3, 2]);
}
