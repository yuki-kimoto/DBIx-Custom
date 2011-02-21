use Test::More;
use strict;
use warnings;

use utf8;
use Encode qw/encode_utf8 decode_utf8/;

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1.25 }
        or plan skip_all => 'DBD::SQLite >= 1.25 required';

    plan 'no_plan';
    use_ok('DBIx::Custom');
}

use FindBin;
use lib "$FindBin::Bin/dbix-custom-core-sqlite";

# Function for test name
sub test { print "# $_[0]\n" }

# Constant varialbes for test
my $CREATE_TABLE = {
    0 => 'create table table1 (key1 char(255), key2 char(255));',
    1 => 'create table table1 (key1 char(255), key2 char(255), key3 char(255), key4 char(255), key5 char(255));',
    2 => 'create table table2 (key1 char(255), key3 char(255));',
    3 => 'create table table1 (key1 Date, key2 datetime);'
};

my $SELECT_SOURCES = {
    0 => 'select * from table1;'
};

my $DROP_TABLE = {
    0 => 'drop table table1'
};

my $NEW_ARGS = {
    0 => {data_source => 'dbi:SQLite:dbname=:memory:'}
};

# Variables
my $dbi;
my $sth;
my $source;
my @sources;
my $select_SOURCE;
my $insert_SOURCE;
my $update_SOURCE;
my $params;
my $sql;
my $result;
my $row;
my @rows;
my $rows;
my $query;
my @queries;
my $select_query;
my $insert_query;
my $update_query;
my $ret_val;
my $infos;
my $model;
my $where;

# Prepare table
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});

test 'DBIx::Custom::Result test';
$source = "select key1, key2 from table1";
$query = $dbi->create_query($source);
$result = $dbi->execute($query);

@rows = ();
while (my $row = $result->fetch) {
    push @rows, [@$row];
}
is_deeply(\@rows, [[1, 2], [3, 4]], "fetch");

$result = $dbi->execute($query);
@rows = ();
while (my $row = $result->fetch_hash) {
    push @rows, {%$row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "fetch_hash");

$result = $dbi->execute($query);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]], "fetch_all");

$result = $dbi->execute($query);
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "fetch_hash_all");

test 'Insert query return value';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$source = "insert into table1 {insert_param key1 key2}";
$query = $dbi->create_query($source);
$ret_val = $dbi->execute($query, param => {key1 => 1, key2 => 2});
ok($ret_val);


test 'Direct query';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$insert_SOURCE = "insert into table1 {insert_param key1 key2}";
$dbi->execute($insert_SOURCE, param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

test 'Filter basic';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice       => sub { $_[0] * 2}, 
                    three_times => sub { $_[0] * 3});

$insert_SOURCE  = "insert into table1 {insert_param key1 key2};";
$insert_query = $dbi->create_query($insert_SOURCE);
$insert_query->filter({key1 => 'twice'});
$dbi->execute($insert_query, param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows = $result->filter({key2 => 'three_times'})->fetch_hash_all;
is_deeply($rows, [{key1 => 2, key2 => 6}], "filter fetch_filter");
$dbi->execute($DROP_TABLE->{0});

test 'Filter in';
$dbi->execute($CREATE_TABLE->{0});
$insert_SOURCE  = "insert into table1 {insert_param key1 key2};";
$insert_query = $dbi->create_query($insert_SOURCE);
$dbi->execute($insert_query, param => {key1 => 2, key2 => 4});
$select_SOURCE = "select * from table1 where {in table1.key1 2} and {in table1.key2 2}";
$select_query = $dbi->create_query($select_SOURCE);
$select_query->filter({'table1.key1' => 'twice'});
$result = $dbi->execute($select_query, param => {'table1.key1' => [1,5], 'table1.key2' => [2,4]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 2, key2 => 4}], "filter");

test 'DBIx::Custom::SQLTemplate basic tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where {= key1} and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->create_query($source);
$result = $dbi->execute($query, param => {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic tag1");

$source = "select * from table1 where {<= key1} and {like key2};";
$query = $dbi->create_query($source);
$result = $dbi->execute($query, param => {key1 => 1, key2 => '%2%'});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic tag2");

test 'DIB::Custom::SQLTemplate in tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where {in key1 2};";
$query = $dbi->create_query($source);
$result = $dbi->execute($query, param => {key1 => [9, 1]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic");

test 'DBIx::Custom::SQLTemplate insert tag';
$dbi->execute("delete from table1");
$insert_SOURCE = 'insert into table1 {insert_param key1 key2 key3 key4 key5}';
$dbi->execute($insert_SOURCE, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});

$result = $dbi->execute($SELECT_SOURCES->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic");

test 'DBIx::Custom::SQLTemplate update tag';
$dbi->execute("delete from table1");
$insert_SOURCE = "insert into table1 {insert_param key1 key2 key3 key4 key5}";
$dbi->execute($insert_SOURCE, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->execute($insert_SOURCE, param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$update_SOURCE = 'update table1 {update_param key1 key2 key3 key4} where {= key5}';
$dbi->execute($update_SOURCE, param => {key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5});

$result = $dbi->execute($SELECT_SOURCES->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5},
                  {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10}], "basic");

test 'Error case';
eval {DBIx::Custom->connect(data_source => 'dbi:SQLit')};
ok($@, "connect error");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
eval{$dbi->create_query("{p }")};
ok($@, "create_query invalid SQL template");

test 'insert';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "basic");

$dbi->execute('delete from table1');
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_bind_filter('twice');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, filter => {key1 => 'three_times'});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "filter");
$dbi->default_bind_filter(undef);

$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, append => '   ');
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], 'insert append');

eval{$dbi->insert(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "invalid argument");

eval{$dbi->insert(table => 'table', param => {';' => 1})};
like($@, qr/safety/);

test 'update';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");
                  
$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 12}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "update key same as search key");

$dbi->update(table => 'table1', param => {key2 => [12]}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "update key same as search key : param is array ref");

$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1},
              filter => {key2 => sub { $_[0] * 2 }});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 22, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "filter");

$result = $dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1}, append => '   ');

eval{$dbi->update(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "invalid argument");

eval{$dbi->update(table => 'table1')};
like($@, qr/where/, "not contain where");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$where = $dbi->where;
$where->clause(['and', '{= key1}', '{= key2}']);
$where->param({key1 => 1, key2 => 2});
$dbi->update(table => 'table1', param => {key1 => 3}, where => $where);
$result = $dbi->select(table => 'table1');
is_deeply($result->fetch_hash_all, [{key1 => 3, key2 => 2}], 'delete() where');

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$where = $dbi->where;
$where->clause(['and', '{= key2}']);
$where->param({key2 => 2});
$dbi->update(table => 'table1', param => {key1 => 3}, where => $where);
$result = $dbi->select(table => 'table1');
is_deeply($result->fetch_hash_all, [{key1 => 3, key2 => 2}], 'delete() where');

eval{$dbi->update(table => 'table1', param => {';' => 1})};
like($@, qr/safety/);

eval{$dbi->update(table => 'table1', param => {'key1' => 1}, where => {';' => 1})};
like($@, qr/safety/);

test 'update_all';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update_all(table => 'table1', param => {key2 => 10}, filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 20, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 20, key3 => 8, key4 => 9, key5 => 10}],
                  "filter");


test 'delete';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "basic");

$dbi->execute("delete from table1;");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->delete(table => 'table1', where => {key2 => 1}, filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "filter");

$dbi->delete(table => 'table1', where => {key1 => 1}, append => '   ');

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1, key2 => 2});
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "delete multi key");

eval{$dbi->delete(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "invalid argument");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$where = $dbi->where;
$where->clause(['and', '{= key1}', '{= key2}']);
$where->param({ke1 => 1, key2 => 2});
$dbi->delete(table => 'table1', where => $where);
$result = $dbi->select(table => 'table1');
is_deeply($result->fetch_hash_all, [{key1 => 3, key2 => 4}], 'delete() where');

test 'delete error';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
eval{$dbi->delete(table => 'table1')};
like($@, qr/"where" must be specified/,
         "where key-value pairs not specified");

eval{$dbi->delete(table => 'table1', where => {';' => 1})};
like($@, qr/safety/);

test 'delete_all';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete_all(table => 'table1');
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [], "basic");


test 'select';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "table");

$rows = $dbi->select(table => 'table1', column => ['key1'])->fetch_hash_all;
is_deeply($rows, [{key1 => 1}, {key1 => 3}], "table and columns and where key");

$rows = $dbi->select(table => 'table1', where => {key1 => 1})->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "table and columns and where key");

$rows = $dbi->select(table => 'table1', column => ['key1'], where => {key1 => 3})->fetch_hash_all;
is_deeply($rows, [{key1 => 3}], "table and columns and where key");

$rows = $dbi->select(table => 'table1', append => "order by key1 desc limit 1")->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "append statement");

$dbi->register_filter(decrement => sub { $_[0] - 1 });
$rows = $dbi->select(table => 'table1', where => {key1 => 2}, filter => {key1 => 'decrement'})
            ->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "filter");

$dbi->execute($CREATE_TABLE->{2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$rows = $dbi->select(
    table => [qw/table1 table2/],
    column => ['table1.key1 as table1_key1', 'table2.key1 as table2_key1', 'key2', 'key3'],
    where   => {'table1.key2' => 2},
    relation  => {'table1.key1' => 'table2.key1'}
)->fetch_hash_all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "relation : exists where");

$rows = $dbi->select(
    table => [qw/table1 table2/],
    column => ['table1.key1 as table1_key1', 'table2.key1 as table2_key1', 'key2', 'key3'],
    relation  => {'table1.key1' => 'table2.key1'}
)->fetch_hash_all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "relation : no exists where");

eval{$dbi->select(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "invalid argument");


test 'fetch filter';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_fetch_filter('twice');
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter({key1 => 'three_times'});
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 3, key2 => 4}, "default_fetch_filter and filter");

test 'filters';
$dbi = DBIx::Custom->new;

is($dbi->filters->{decode_utf8}->(encode_utf8('あ')),
   'あ', "decode_utf8");

is($dbi->filters->{encode_utf8}->('あ'),
   encode_utf8('あ'), "encode_utf8");

test 'transaction';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->dbh->begin_work;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$dbi->dbh->commit;
$result = $dbi->select(table => 'table1');
is_deeply(scalar $result->fetch_hash_all, [{key1 => 1, key2 => 2}, {key1 => 2, key2 => 3}],
          "commit");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->dbh->begin_work(0);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->dbh->rollback;

$result = $dbi->select(table => 'table1');
ok(! $result->fetch_first, "rollback");

test 'cache';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$source = 'select * from table1 where {= key1} and {= key2};';
$dbi->create_query($source);
is_deeply($dbi->{_cached}->{$source}, 
          {sql => "select * from table1 where key1 = ? and key2 = ?;", columns => ['key1', 'key2'], tables => []}, "cache");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->{_cached} = {};
$dbi->cache(0);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is(scalar keys %{$dbi->{_cached}}, 0, 'not cache');

test 'execute';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
{
    local $Carp::Verbose = 0;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/\Qselect * frm table1;/, "fail prepare");
    like($@, qr/\.t /, "fail : not verbose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/Custom.*\.t /s, "fail : verbose");
}

eval{$dbi->execute('select * from table1', no_exists => 1)};
like($@, qr/\Q"no_exists" is invalid argument/, "invald SQL");

$query = $dbi->create_query('select * from table1 where {= key1}');
$dbi->dbh->disconnect;
eval{$dbi->execute($query, param => {key1 => {a => 1}})};
ok($@, "execute fail");

{
    local $Carp::Verbose = 0;
    eval{$dbi->create_query('select * from table1 where {0 key1}')};
    like($@, qr/\Q.t /, "caller spec : not vebose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->create_query('select * from table1 where {0 key1}')};
    like($@, qr/QueryBuilder.*\.t /s, "caller spec : not vebose");
}


test 'transaction';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});

$dbi->begin_work;

eval {
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    die "Error";
    $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
};

$dbi->rollback if $@;

$result = $dbi->select(table => 'table1');
$rows = $result->fetch_hash_all;
is_deeply($rows, [], "rollback");

$dbi->begin_work;

eval {
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
};

$dbi->commit unless $@;

$result = $dbi->select(table => 'table1');
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "commit");

$dbi->dbh->{AutoCommit} = 0;
eval{ $dbi->begin_work };
ok($@, "exception");
$dbi->dbh->{AutoCommit} = 1;


test 'method';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->method(
    one => sub { 1 }
);
$dbi->method(
    two => sub { 2 }
);
$dbi->method({
    twice => sub {
        my $self = shift;
        return $_[0] * 2;
    }
});

is($dbi->one, 1, "first");
is($dbi->two, 2, "second");
is($dbi->twice(5), 10 , "second");

eval {$dbi->XXXXXX};
ok($@, "not exists");

test 'out filter';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3});
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'three_times'}, 
              'key2' => {out => 'three_times', in => 'twice'});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_SOURCES->{0});
$row   = $result->fetch_hash_first;
is_deeply($row, {key1 => 2, key2 => 6}, "insert");
$result = $dbi->select(table => 'table1');
$row   = $result->fetch_hash_first;
is_deeply($row, {key1 => 6, key2 => 12}, "insert");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3});
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'three_times'}, 
              'key2' => {out => 'three_times', in => 'twice'});
$dbi->apply_filter(
    'table1', 'key1' => {out => undef}
); 
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_SOURCES->{0});
$row   = $result->fetch_hash_first;
is_deeply($row, {key1 => 1, key2 => 6}, "insert");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, filter => {key1 => undef});
$dbi->update(table => 'table1', param => {key1 => 2}, where => {key2 => 2});
$result = $dbi->execute($SELECT_SOURCES->{0});
$row   = $result->fetch_hash_first;
is_deeply($row, {key1 => 4, key2 => 2}, "update");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1=> undef});
$dbi->delete(table => 'table1', where => {key1 => 1});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [], "delete");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1 => undef});
$result = $dbi->select(table => 'table1', where => {key1 => 1});
$result->filter({'key2' => 'twice'});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 4, key2 => 4}], "select");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1 => undef});
$result = $dbi->execute("select * from table1 where {= key1} and {= key2};",
                        param => {key1 => 1, key2 => 2},
                        table => ['table1']);
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 4, key2 => 2}], "execute");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1 => undef});
$result = $dbi->execute("select * from {table table1} where {= key1} and {= key2};",
                        param => {key1 => 1, key2 => 2});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 4, key2 => 2}], "execute table tag");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->execute($CREATE_TABLE->{2});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3 });
$dbi->apply_filter(
    'table1', 'key2' => {out => 'twice', in => 'twice'}
);
$dbi->apply_filter(
    'table2', 'key3' => {out => 'three_times', in => 'three_times'}
);
$dbi->insert(table => 'table1', param => {key1 => 5, key2 => 2}, filter => {key2 => undef});
$dbi->insert(table => 'table2', param => {key1 => 5, key3 => 6}, filter => {key3 => undef});
$result = $dbi->select(
     table => ['table1', 'table2'],
     column => ['key2', 'key3'],
     where => {'table1.key2' => 1, 'table2.key3' => 2}, relation => {'table1.key1' => 'table2.key1'});

$result->filter({'key2' => 'twice'});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key2 => 4, key3 => 18}], "select : join");

$result = $dbi->select(
     table => ['table1', 'table2'],
     column => ['key2', 'key3'],
     where => {'key2' => 1, 'key3' => 2}, relation => {'table1.key1' => 'table2.key1'});

$result->filter({'key2' => 'twice'});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key2 => 4, key3 => 18}], "select : join : omit");

test 'each_column';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{2});
$dbi->execute($CREATE_TABLE->{3});

$infos = [];
$dbi->each_column(sub {
    my ($self, $table, $column, $cinfo) = @_;
    
    if ($table =~ /^table/) {
         my $info = [$table, $column, $cinfo->{COLUMN_NAME}];
         push @$infos, $info;
    }
});
$infos = [sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @$infos];
is_deeply($infos, 
    [
        ['table1', 'key1', 'key1'],
        ['table1', 'key2', 'key2'],
        ['table2', 'key1', 'key1'],
        ['table2', 'key3', 'key3']
    ]
    
);

test 'limit';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 6});
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
)->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 2 1}"
)->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 4},{key1 => 1, key2 => 6}]);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 1}"
)->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

test 'connect super';
{
    package MyDBI;
    
    use base 'DBIx::Custom';
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        return $self;
    }
    
    sub new {
        my $self = shift->SUPER::new(@_);
        
        return $self;
    }
}

$dbi = MyDBI->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);

$dbi = MyDBI->new($NEW_ARGS->{0});
$dbi->connect;
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);

{
    package MyDBI2;
    
    use base 'DBIx::Custom';
    sub connect {
        my $self = shift->SUPER::new(@_);
        $self->connect;
        
        return $self;
    }
}

$dbi = MyDBI->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);

test 'end_filter';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->end_filter(key1 => sub { $_[0] * 3 }, key2 => sub { $_[0] * 5 });
$row = $result->fetch_first;
is_deeply($row, [6, 40]);

$dbi->register_filter(five_times => sub { $_[0] * 5 });
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->end_filter({key1 => sub { $_[0] * 3 }, key2 => 'five_times' });
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 6, key2 => 40});

$dbi->register_filter(five_times => sub { $_[0] * 5 });
$dbi->apply_filter('table1',
    key1 => {end => sub { $_[0] * 3 } },
    key2 => {end => 'five_times'}
);
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 6, key2 => 40}, 'apply_filter');

$dbi->register_filter(five_times => sub { $_[0] * 5 });
$dbi->apply_filter('table1',
    key1 => {end => sub { $_[0] * 3 } },
    key2 => {end => 'five_times'}
);
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->filter(key1 => undef);
$result->end_filter(key1 => undef);
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 1, key2 => 40}, 'apply_filter overwrite');

test 'empty where select';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1', where => {});
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 1, key2 => 2});

test 'select query option';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$query = $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, query => 1);
is(ref $query, 'DBIx::Custom::Query');
$query = $dbi->update(table => 'table1', where => {key1 => 1}, param => {key2 => 2}, query => 1);
is(ref $query, 'DBIx::Custom::Query');
$query = $dbi->delete(table => 'table1', where => {key1 => 1}, query => 1);
is(ref $query, 'DBIx::Custom::Query');
$query = $dbi->select(table => 'table1', where => {key1 => 1, key2 => 2}, query => 1);
is(ref $query, 'DBIx::Custom::Query');

test 'DBIx::Custom::Where';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$where = $dbi->where->clause(['and', '{= key1}', '{= key2}']);
is("$where", "where ( {= key1} and {= key2} )", 'no param');

$where = $dbi->where
             ->clause(['and', '{= key1}', '{= key2}'])
             ->param({key1 => 1});

$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(['and', '{= key1}', '{= key2}'])
             ->param({key1 => 1, key2 => 2});
$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(['and', '{= key1}', '{= key2}'])
             ->param({});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where
             ->clause(['and', ['or', '{> key1}', '{< key1}'], '{= key2}'])
             ->param({key1 => [0, 3], key2 => 2});
$result = $dbi->select(
    table => 'table1',
    where => $where,
); 
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where;
$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

eval {
$where = $dbi->where
             ->clause(['uuu']);
$result = $dbi->select(
    table => 'table1',
    where => $where
);
};
ok($@);

$where = $dbi->where;
is("$where", '');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 2])
             ->param({key1 => [1, 3]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 2])
             ->param({key1 => [1]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 2])
             ->param({key1 => 1});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause('{= key1}')
             ->param({key1 => 1});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause('{= key1} {= key2}')
             ->param({key1 => 1});
eval{$where->to_string};
like($@, qr/one column/);

$where = $dbi->where
             ->clause('{= key1}')
             ->param([]);
eval{$where->to_string};
like($@, qr/Parameter/);

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [$dbi->not_exists, 1, 3]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [1, $dbi->not_exists, 3]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [1, 3, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [1, $dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [$dbi->not_exists, 1, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [$dbi->not_exists, $dbi->not_exists, 1]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => [$dbi->not_exists, $dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('{= key1}') x 3])
             ->param({key1 => []});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [2, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [$dbi->not_exists, 2]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [$dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2},{key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [0, 2]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

test 'dbi_option default';
$dbi = DBIx::Custom->new;
is_deeply($dbi->dbi_option, {});

test 'register_tag_processor';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->register_tag_processor(
    a => sub { 1 }
);
is($dbi->query_builder->tag_processors->{a}->(), 1);

test 'register_tag';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->register_tag(
    b => sub { 2 }
);
is($dbi->query_builder->tags->{b}->(), 2);

test 'table not specify exception';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
eval {$dbi->insert};
like($@, qr/table/);
eval {$dbi->update};
like($@, qr/table/);
eval {$dbi->delete};
like($@, qr/table/);
eval {$dbi->select};
like($@, qr/table/);


test 'more tests';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
eval{$dbi->apply_filter('table', 'column', [])};
like($@, qr/apply_filter/);

eval{$dbi->apply_filter('table', 'column', {outer => 2})};
like($@, qr/apply_filter/);

$dbi->apply_filter(

);
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->apply_filter('table1', 'key2', 
                   {in => sub { $_[0] * 3 }, out => sub { $_[0] * 2 }});
$rows = $dbi->select(table => 'table1', where => {key2 => 1})->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 6}]);

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->apply_filter('table1', 'key2', {});
$rows = $dbi->select(table => 'table1', where => {key2 => 2})->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
eval {$dbi->apply_filter('table1', 'key2', {out => 'no'})};
like($@, qr/not registered/);
eval {$dbi->apply_filter('table1', 'key2', {in => 'no'})};
like($@, qr/not registered/);
$dbi->method({one => sub { 1 }});
is($dbi->one, 1);

eval{DBIx::Custom->connect()};
like($@, qr/connect/);

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2},
             filter => {key1 => 'twice'});
$row = $dbi->select(table => 'table1')->fetch_hash_first;
is_deeply($row, {key1 => 2, key2 => 2});
eval {$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2},
             filter => {key1 => 'no'}) };
like($@, qr//);

$dbi->register_filter(one => sub { });
$dbi->default_fetch_filter('one');
ok($dbi->default_fetch_filter);
$dbi->default_bind_filter('one');
ok($dbi->default_bind_filter);
eval{$dbi->default_fetch_filter('no')};
like($@, qr/not registered/);
eval{$dbi->default_bind_filter('no')};
like($@, qr/not registered/);
$dbi->default_bind_filter(undef);
ok(!defined $dbi->default_bind_filter);
$dbi->default_fetch_filter(undef);
ok(!defined $dbi->default_fetch_filter);
eval {$dbi->execute('select * from table1 {= author') };
like($@, qr/Tag not finished/);

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(one => sub { 1 });
$result = $dbi->select(table => 'table1');
eval {$result->filter(key1 => 'no')};
like($@, qr/not registered/);
eval {$result->end_filter(key1 => 'no')};
like($@, qr/not registered/);
$result->default_filter(undef);
ok(!defined $result->default_filter);
$result->default_filter('one');
is($result->default_filter->(), 1);

test 'dbi_option';
$dbi = DBIx::Custom->connect(data_source => 'dbi:SQLite:dbname=:memory:',
                             dbi_option => {PrintError => 1});
ok($dbi->dbh->{PrintError});
$dbi = DBIx::Custom->connect(data_source => 'dbi:SQLite:dbname=:memory:',
                             dbi_options => {PrintError => 1});
ok($dbi->dbh->{PrintError});

test 'DBIx::Custom::Result stash()';
$result = DBIx::Custom::Result->new;
is_deeply($result->stash, {}, 'default');
$result->stash->{foo} = 1;
is($result->stash->{foo}, 1, 'get and set');

test 'filter __ expression';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute('create table company (id, name, location_id)');
$dbi->execute('create table location (id, name)');
$dbi->apply_filter('location',
  name => {in => sub { uc $_[0] } }
);

$dbi->insert(table => 'company', param => {id => 1, name => 'a', location_id => 2});
$dbi->insert(table => 'location', param => {id => 2, name => 'b'});

$result = $dbi->select(
    table => ['company', 'location'], relation => {'company.location_id' => 'location.id'},
    column => ['location.name as location__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'company', relation => {'company.location_id' => 'location.id'},
    column => ['location.name as location__name']
);
is($result->fetch_first->[0], 'B');

test 'selection';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(selection => '* from table1', where => {key1 => 1});
is_deeply($result->fetch_hash_all, [{key1 => 1, key2 => 2}]);

test 'Model class';
use MyDBI1;
$dbi = MyDBI1->connect($NEW_ARGS->{0});
$dbi->execute("create table book (title, author)");
$model = $dbi->model('book');
$model->insert({title => 'a', author => 'b'});
is_deeply($model->list->fetch_hash_all, [{title => 'a', author => 'b'}], 'basic');
$dbi->execute("create table company (name)");
$model = $dbi->model('company');
$model->insert({name => 'a'});
is_deeply($model->list->fetch_hash_all, [{name => 'a'}], 'basic');
is($dbi->models->{'book'}, $dbi->model('book'));
is($dbi->models->{'company'}, $dbi->model('company'));

$dbi->model('book');
eval{$dbi->model('book')->no_exists};
like($@, qr/locate/);

{
    package MyDBI4;

    use strict;
    use warnings;

    use base 'DBIx::Custom';

    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model(
            MyModel2 => [
                'book',
                {company => 'Company'}
            ]
        );
    }

    package MyModel2::Base1;

    use strict;
    use warnings;

    use base 'DBIx::Custom::Model';

    package MyModel2::book;

    use strict;
    use warnings;

    use base 'MyModel2::Base1';

    sub insert {
        my ($self, $param) = @_;
        
        return $self->SUPER::insert(param => $param);
    }

    sub list { shift->select; }

    package MyModel2::Company;

    use strict;
    use warnings;

    use base 'MyModel2::Base1';

    sub insert {
        my ($self, $param) = @_;
        
        return $self->SUPER::insert(param => $param);
    }

    sub list { shift->select; }
}
$dbi = MyDBI4->connect($NEW_ARGS->{0});
$dbi->execute("create table book (title, author)");
$model = $dbi->model('book');
$model->insert({title => 'a', author => 'b'});
is_deeply($model->list->fetch_hash_all, [{title => 'a', author => 'b'}], 'basic');
$dbi->execute("create table company (name)");
$model = $dbi->model('company');
$model->insert({name => 'a'});
is_deeply($model->list->fetch_hash_all, [{name => 'a'}], 'basic');

{
     package MyDBI5;

    use strict;
    use warnings;

    use base 'DBIx::Custom';

    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel4');
    }
}
$dbi = MyDBI5->connect($NEW_ARGS->{0});
$dbi->execute("create table company (name)");
$model = $dbi->model('company');
$model->insert({name => 'a'});
is_deeply($model->list->fetch_hash_all, [{name => 'a'}], 'include all model');
$model = $dbi->model('book');
is_deeply($model->list->fetch_hash_all, [{name => 'a'}], 'include all model');

test 'primary_key';
use MyDBI1;
$dbi = MyDBI1->connect($NEW_ARGS->{0});
$model = $dbi->model('book');
$model->primary_key(['id', 'number']);
is_deeply($model->primary_key, ['id', 'number']);

test 'columns';
use MyDBI1;
$dbi = MyDBI1->connect($NEW_ARGS->{0});
$model = $dbi->model('book');
$model->columns(['id', 'number']);
is_deeply($model->columns, ['id', 'number']);

test 'setup_model';
use MyDBI1;
$dbi = MyDBI1->connect($NEW_ARGS->{0});
$dbi->execute('create table book (id)');
$dbi->execute('create table company (id, name);');
$dbi->execute('create table test (id, name, primary key (id, name));');
$dbi->setup_model;
is_deeply($dbi->model('book')->columns, ['id']);
is_deeply($dbi->model('company')->columns, ['id', 'name']);

test 'delete_at';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->delete_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2],
);
is_deeply($dbi->select(table => 'table1')->fetch_hash_all, []);

$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->delete_at(
    table => 'table1',
    primary_key => 'key1',
    where => 1,
);
is_deeply($dbi->select(table => 'table1')->fetch_hash_all, []);

$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->delete_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    param => {key1 => 1, key2 => 2},
);
is_deeply($dbi->select(table => 'table1')->fetch_hash_all, []);


test 'update_at';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2],
    param => {key3 => 4}
);
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);
is($dbi->select(table => 'table1')->fetch_hash_first->{key2}, 2);
is($dbi->select(table => 'table1')->fetch_hash_first->{key3}, 4);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update_at(
    table => 'table1',
    primary_key => 'key1',
    where => 1,
    param => {key3 => 4}
);
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);
is($dbi->select(table => 'table1')->fetch_hash_first->{key2}, 2);
is($dbi->select(table => 'table1')->fetch_hash_first->{key3}, 4);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    param => {key1 => 1, key2 => 2, key3 => 4},
);
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);
is($dbi->select(table => 'table1')->fetch_hash_first->{key2}, 2);
is($dbi->select(table => 'table1')->fetch_hash_first->{key3}, 4);


test 'select_at';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2]
);
$row = $result->fetch_hash_first;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select_at(
    table => 'table1',
    primary_key => 'key1',
    where => 1,
);
$row = $result->fetch_hash_first;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    param => {key1 => 1, key2 => 2},
);
$row = $result->fetch_hash_first;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);


test 'columns';
use MyDBI1;
$dbi = MyDBI1->connect($NEW_ARGS->{0});
$model = $dbi->model('book');
$model->relation({'book.id' => 'company.id'});
is_deeply($model->relation, {'book.id' => 'company.id'});


test 'model delete_at';
{
    package MyDBI6;
    
    use base 'DBIx::Custom';
    
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel5');
        
        return $self;
    }
}
$dbi = MyDBI6->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1')->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => 'table1')->fetch_hash_all, []);


test 'update_at';
$dbi = MyDBI6->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1')->update_at(
    where => [1, 2],
    param => {key3 => 4}
);
$result = $dbi->model('table1')->select;
$row = $result->fetch_hash_first;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 4);

test 'select_at';
$dbi = MyDBI6->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->model('table1')->select_at(where => [1, 2]);
$row = $result->fetch_hash_first;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);
