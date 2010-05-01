use Test::More;
use strict;
use warnings;
use utf8;

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1.25 }
        or plan skip_all => 'DBD::SQLite >= 1.25 required';

    plan 'no_plan';
    use_ok('DBIx::Custom::SQLite');
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


# Variables for tests
my $dbi;
my $ret_val;
my $rows;
my $db_file;
my $id;

test 'connect_memory';
$dbi = DBIx::Custom::SQLite->new;
$dbi->connect_memory;
$ret_val = $dbi->query($CREATE_TABLE->{0});
ok(defined $ret_val, $test);
$dbi->insert('table1', {key1 => 'a', key2 => 2});
$rows = $dbi->select('table1', {where => {key1 => 'a'}})->fetch_hash_all;
is_deeply($rows, [{key1 => 'a', key2 => 2}], "$test : select rows");

test 'connect_memory error';
eval{$dbi->connect_memory};
like($@, qr/Already connected/, "$test : already connected");

test 'reconnect_memory';
$dbi = DBIx::Custom::SQLite->new;
$dbi->reconnect_memory;
$ret_val = $dbi->query($CREATE_TABLE->{0});
ok(defined $ret_val, "$test : connect first");
$dbi->reconnect_memory;
$ret_val = $dbi->query($CREATE_TABLE->{2});
ok(defined $ret_val, "$test : connect first");

test 'connect';
$db_file  = 't/test.db';
unlink $db_file if -f $db_file;
$dbi = DBIx::Custom::SQLite->new(database => $db_file);
$dbi->connect;
ok(-f $db_file, "$test : database file");
$ret_val = $dbi->query($CREATE_TABLE->{0});
ok(defined $ret_val, "$test : database");
$dbi->disconnect;
unlink $db_file if -f $db_file;

test 'last_insert_rowid';
$dbi = DBIx::Custom::SQLite->new;
$dbi->connect_memory;
$ret_val = $dbi->query($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
is($dbi->last_insert_rowid, 1, "$test: first");
$dbi->insert('table1', {key1 => 1, key2 => 2});
is($dbi->last_insert_rowid, 2, "$test: second");
$dbi->disconnect;
