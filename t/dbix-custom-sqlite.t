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
sub test { "# $_[0]\n" }

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
$dbi = DBIx::Custom::SQLite->connect_memory;
$ret_val = $dbi->execute($CREATE_TABLE->{0});
ok(defined $ret_val);
$dbi->insert(table => 'table1', param => {key1 => 'a', key2 => 2});
$rows = $dbi->select(table => 'table1', where => {key1 => 'a'})->fetch_hash_all;
is_deeply($rows, [{key1 => 'a', key2 => 2}], "select rows");

test 'connect';
$db_file  = 't/test.db';
unlink $db_file if -f $db_file;
$dbi = DBIx::Custom::SQLite->new(database => $db_file);
$dbi->connect;
ok(-f $db_file, "database file");
$ret_val = $dbi->execute($CREATE_TABLE->{0});
ok(defined $ret_val, "database");
$dbi->dbh->disconnect;

unlink $db_file if -f $db_file;
$dbi = DBIx::Custom::SQLite->connect(database => $db_file);
ok($dbi, "called from class name");

unlink $db_file if -f $db_file;
$dbi = DBIx::Custom::SQLite->connect(data_source => "dbi:SQLite:dbname=$db_file");
ok($dbi, "specified data source");
