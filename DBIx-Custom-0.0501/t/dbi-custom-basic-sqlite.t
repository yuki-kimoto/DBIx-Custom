use Test::More;
use strict;
use warnings;
use utf8;
use Encode qw/decode encode/;

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1 }
        or plan skip_all => 'DBD::SQLite >= 1.00 required';

    plan 'no_plan';
    use_ok('DBIx::Custom');
}

# Function for test name
my $test;
sub test {
    $test = shift;
}

# Constant varialbes for test
my $CREATE_TABLE = {
    0 => 'create table table1 (key1 char(255), key2 char(255));',
    1 => 'create table table1 (key1 char(255), key2 char(255), key3 char(255), key4 char(255), key5 char(255));',
    2 => 'create table table2 (key1 char(255), key3 char(255));'
};

my $SELECT_TMPL = {
    0 => 'select * from table1;'
};

my $DROP_TABLE = {
    0 => 'drop table table1'
};

my $NEW_ARGS = {
    0 => {data_source => 'dbi:SQLite:dbname=:memory:'}
};

# Variables for test
my $dbi;
my $decoded_str;
my $encoded_str;
my $array;

use DBIx::Custom::Basic;

test 'Filter';
$dbi = DBIx::Custom::Basic->new($NEW_ARGS->{0});
ok($dbi->filters->{encode_utf8}, "$test : exists default_bind_filter");
ok($dbi->filters->{decode_utf8}, "$test : exists default_fetch_filter");

$dbi->utf8_filter_on;
is($dbi->bind_filter, $dbi->filters->{encode_utf8}, 'default bind filter');
is($dbi->fetch_filter, $dbi->filters->{decode_utf8}, 'default fetch filter');

$decoded_str = 'あ';
$encoded_str = $dbi->bind_filter->($decoded_str);
is($encoded_str, encode('UTF-8', $decoded_str), "$test : encode utf8");
is($decoded_str, $dbi->fetch_filter->($encoded_str), "$test : fetch_filter");

$decoded_str = 'a';
$encoded_str = $dbi->bind_filter->($decoded_str);
is($encoded_str, encode('UTF-8', $decoded_str), "$test : upgrade and encode utf8");
is($decoded_str, $dbi->fetch_filter->($encoded_str), "$test : fetch_filter");
