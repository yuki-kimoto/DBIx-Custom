use Test::More;
use strict;
use warnings;
use utf8;
use Encode qw/encode_utf8 decode_utf8/;
use FindBin;
use lib "$FindBin::Bin/common";

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1.25 }
        or plan skip_all => 'DBD::SQLite >= 1.25 required';

    plan 'no_plan';
    use_ok('DBIx::Custom');
}

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};
sub test { print "# $_[0]\n" }

use DBIx::Custom;
{
    package DBIx::Custom;
    has dsn => sub { 'dbi:SQLite:dbname=:memory:' }
}

# Constant
my $create_table1 = 'create table table1 (key1 varchar, key2 varchar);';
my $create_table_reserved = 'create table "table" ("select" varchar, "update" varchar)';
my $q = '"';
my $p = '"';

# Variables
my $dbi;
my $result;
my $row;
my $rows;
my $binary;

# Prepare table
$dbi = DBIx::Custom->connect;

### SQLite only test
test 'dbi_option default';
$dbi = DBIx::Custom->new;
is_deeply($dbi->dbi_option, {});


test 'prefix';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1 (key1 varchar, key2 varchar, primary key(key1));');
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 1, key2 => 4}, table => 'table1', prefix => 'or replace');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1 (key1 varchar, key2 varchar, primary key(key1));');
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->update({key2 => 4}, table => 'table1',
  where => {key1 => 1}, prefix => 'or replace');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");


test 'quote';
$dbi = DBIx::Custom->connect;
$dbi->quote('"');
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert({select => 1}, table => 'table');
$dbi->delete(table => 'table', where => {select => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [], "reserved word");

test 'finish statement handle';
$dbi = DBIx::Custom->connect;
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');

$result = $dbi->select(table => 'table1');
$row = $result->fetch_first;
is_deeply($row, [1, 2], "row");
$row = $result->fetch;
ok(!$row, "finished");

$result = $dbi->select(table => 'table1');
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 1, key2 => 2}, "row");
$row = $result->fetch_hash;
ok(!$row, "finished");

$dbi->execute('create table table2 (key1, key2);');
$result = $dbi->select(table => 'table2');
$row = $result->fetch_hash_first;
ok(!$row, "no row fetch");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
$dbi->insert({key1 => 5, key2 => 6}, table => 'table1');
$dbi->insert({key1 => 7, key2 => 8}, table => 'table1');
$dbi->insert({key1 => 9, key2 => 10}, table => 'table1');
$result = $dbi->select(table => 'table1');
$rows = $result->fetch_multi(2);
is_deeply($rows, [[1, 2],
                  [3, 4]], "fetch_multi first");
$rows = $result->fetch_multi(2);
is_deeply($rows, [[5, 6],
                  [7, 8]], "fetch_multi secound");
$rows = $result->fetch_multi(2);
is_deeply($rows, [[9, 10]], "fetch_multi third");
$rows = $result->fetch_multi(2);
ok(!$rows);

$result = $dbi->select(table => 'table1');
eval {$result->fetch_multi};
like($@, qr/Row count must be specified/, "Not specified row count");

$result = $dbi->select(table => 'table1');
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "fetch_multi first");
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{key1 => 5, key2 => 6},
                  {key1 => 7, key2 => 8}], "fetch_multi secound");
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{key1 => 9, key2 => 10}], "fetch_multi third");
$rows = $result->fetch_hash_multi(2);
ok(!$rows);

$result = $dbi->select(table => 'table1');
eval {$result->fetch_hash_multi};
like($@, qr/Row count must be specified/, "Not specified row count");


test 'type option'; # DEPRECATED!
$dbi = DBIx::Custom->connect(
    data_source => 'dbi:SQLite:dbname=:memory:',
    dbi_option => {
        $DBD::SQLite::VERSION > 1.26 ? (sqlite_unicode => 1) : (unicode => 1)
    }
);
$binary = pack("I3", 1, 2, 3);
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1(key1, key2)');
$dbi->insert({key1 => $binary, key2 => 'あ'}, table => 'table1', type => [key1 => DBI::SQL_BLOB]);
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);

test 'type_rule from';
$dbi = DBIx::Custom->connect;
$dbi->type_rule(
    from1 => {
        date => sub { uc $_[0] }
    }
);
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->insert({key1 => 'a'}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->fetch_first->[0], 'A');

$result = $dbi->select(table => 'table1');
is($result->one->{key1}, 'A');

test 'select limit';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
$rows = $dbi->select(table => 'table1', append => "order by key1 desc limit 1")->all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "append statement");



# DEPRECATED! test
test 'filter __ expression';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table2') };
eval { $dbi->execute('drop table table3') };
$dbi->execute('create table table2 (id, name, table3_id)');
$dbi->execute('create table table3 (id, name)');
$dbi->apply_filter('table3',
  name => {in => sub { uc $_[0] } }
);

$dbi->insert({id => 1, name => 'a', table3_id => 2}, table => 'table2');
$dbi->insert({id => 2, name => 'b'}, table => 'table3');

$result = $dbi->select(
    table => ['table2', 'table3'], relation => {'table2.table3_id' => 'table3.id'},
    column => ['table3.name as table3__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'table2', relation => {'table2.table3_id' => 'table3.id'},
    column => ['table3.name as table3__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'table2', relation => {'table2.table3_id' => 'table3.id'},
    column => ['table3.name as "table3.name"']
);
is($result->fetch_first->[0], 'B');

test 'reserved_word_quote';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->reserved_word_quote('"');
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->apply_filter('table', update => {out => sub { $_[0] * 3}});
$dbi->insert({select => 1}, table => 'table');
$dbi->update({update => 2}, table => 'table', where => {'table.select' => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 6}], "reserved word");

test 'limit tag';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 1, key2 => 4}, table => 'table1');
$dbi->insert({key1 => 1, key2 => 6}, table => 'table1');
$dbi->register_tag(
    limit => sub {
        my ($count, $offset) = @_;
        
        my $s = '';
        $s .= "limit $count";
        $s .= " offset $offset" if defined $offset;
        
        return [$s, []];
    }
);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 1 0}"
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 2 1}"
)->all;
is_deeply($rows, [{key1 => 1, key2 => 4},{key1 => 1, key2 => 6}]);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 1}"
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

test 'join function';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table table1") };
eval { $dbi->execute("drop table table2") };
$dbi->execute($create_table1);
$dbi->execute("create table table2 (key1, key3)");
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 1, key3 => 4}, table => 'table2');
$dbi->insert({key1 => 1, key3 => 1}, table => 'table2');
$result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
        "left outer join table2 on coalesce(table1.key1, 0) = coalesce(table2.key1, 0) and table2.key3 > '3'"
    ]
);
is_deeply($result->all, [{"table2.key3" => 4}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table table1") };
eval { $dbi->execute("drop table table2") };
$dbi->execute($create_table1);
$dbi->execute("create table table2 (key1, key3)");
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 1, key3 => 4}, table => 'table2');
$dbi->insert({key1 => 1, key3 => 1}, table => 'table2');
$result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
        "left outer join table2 on table2.key3 > '3' and coalesce(table1.key1, 0) = coalesce(table2.key1, 0)"
    ]
);
is_deeply($result->all, [{"table2.key3" => 4}]);
