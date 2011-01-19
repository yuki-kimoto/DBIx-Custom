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

# Function for test name
my $test;
sub test {
    $test = shift;
}

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
my $table;
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
is_deeply(\@rows, [[1, 2], [3, 4]], "$test : fetch");

$result = $dbi->execute($query);
@rows = ();
while (my $row = $result->fetch_hash) {
    push @rows, {%$row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : fetch_hash");

$result = $dbi->execute($query);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]], "$test : fetch_all");

$result = $dbi->execute($query);
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : fetch_hash_all");

test 'Insert query return value';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$source = "insert into table1 {insert_param key1 key2}";
$query = $dbi->create_query($source);
$ret_val = $dbi->execute($query, param => {key1 => 1, key2 => 2});
ok($ret_val, $test);


test 'Direct query';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$insert_SOURCE = "insert into table1 {insert_param key1 key2}";
$dbi->execute($insert_SOURCE, param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], $test);

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
is_deeply($rows, [{key1 => 2, key2 => 6}], "$test : filter fetch_filter");
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
is_deeply($rows, [{key1 => 2, key2 => 4}], "$test : filter");

test 'DBIx::Custom::SQLTemplate basic tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where {= key1} and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->create_query($source);
$result = $dbi->execute($query, param => {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag1");

$source = "select * from table1 where {<= key1} and {like key2};";
$query = $dbi->create_query($source);
$result = $dbi->execute($query, param => {key1 => 1, key2 => '%2%'});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag2");

test 'DIB::Custom::SQLTemplate in tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where {in key1 2};";
$query = $dbi->create_query($source);
$result = $dbi->execute($query, param => {key1 => [9, 1]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic");

test 'DBIx::Custom::SQLTemplate insert tag';
$dbi->execute("delete from table1");
$insert_SOURCE = 'insert into table1 {insert_param key1 key2 key3 key4 key5}';
$dbi->execute($insert_SOURCE, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});

$result = $dbi->execute($SELECT_SOURCES->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic");

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
                  {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10}], "$test : basic");

test 'Error case';
eval {DBIx::Custom->connect(data_source => 'dbi:SQLit')};
ok($@, "$test : connect error");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
eval{$dbi->create_query("{p }")};
ok($@, "$test : create_query invalid SQL template");

test 'insert';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : basic");

$dbi->execute('delete from table1');
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_bind_filter('twice');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, filter => {key1 => 'three_times'});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : filter");
$dbi->default_bind_filter(undef);

$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, append => '   ');
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], 'insert append');

eval{$dbi->insert(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "$test: invalid argument");


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
                  "$test : basic");
                  
$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 12}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : update key same as search key");

$dbi->update(table => 'table1', param => {key2 => [12]}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : update key same as search key : param is array ref");

$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1},
              filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 22, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : filter");

$result = $dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1}, append => '   ');

eval{$dbi->update(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "$test: invalid argument");

eval{$dbi->update(table => 'table1')};
like($@, qr/where/, "$test: not contain where");


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
                  "$test : filter");


test 'delete';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : basic");

$dbi->execute("delete from table1;");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->delete(table => 'table1', where => {key2 => 1}, filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : filter");

$dbi->delete(table => 'table1', where => {key1 => 1}, append => '   ');

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1, key2 => 2});
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : delete multi key");

eval{$dbi->delete(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "$test: invalid argument");


test 'delete error';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
eval{$dbi->delete(table => 'table1')};
like($@, qr/"where" argument must be specified and contains the pairs of column name and value/,
         "$test : where key-value pairs not specified");

test 'delete_all';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete_all(table => 'table1');
$result = $dbi->execute($SELECT_SOURCES->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [], "$test : basic");


test 'select';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "$test : table");

$rows = $dbi->select(table => 'table1', column => ['key1'])->fetch_hash_all;
is_deeply($rows, [{key1 => 1}, {key1 => 3}], "$test : table and columns and where key");

$rows = $dbi->select(table => 'table1', where => {key1 => 1})->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "$test : table and columns and where key");

$rows = $dbi->select(table => 'table1', where => ['{= key1} and {= key2}', {key1 => 1, key2 => 2}])->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "$test : table and columns and where string");

$rows = $dbi->select(table => 'table1', column => ['key1'], where => {key1 => 3})->fetch_hash_all;
is_deeply($rows, [{key1 => 3}], "$test : table and columns and where key");

$rows = $dbi->select(table => 'table1', append => "order by key1 desc limit 1")->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : append statement");

$dbi->register_filter(decrement => sub { $_[0] - 1 });
$rows = $dbi->select(table => 'table1', where => {key1 => 2}, filter => {key1 => 'decrement'})
            ->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "$test : filter");

$dbi->execute($CREATE_TABLE->{2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$rows = $dbi->select(
    table => [qw/table1 table2/],
    column => ['table1.key1 as table1_key1', 'table2.key1 as table2_key1', 'key2', 'key3'],
    where   => {'table1.key2' => 2},
    relation  => {'table1.key1' => 'table2.key1'}
)->fetch_hash_all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "$test : relation : exists where");

$rows = $dbi->select(
    table => [qw/table1 table2/],
    column => ['table1.key1 as table1_key1', 'table2.key1 as table2_key1', 'key2', 'key3'],
    relation  => {'table1.key1' => 'table2.key1'}
)->fetch_hash_all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "$test : relation : no exists where");

eval{$dbi->select(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "$test: invalid argument");


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
is_deeply($row, {key1 => 3, key2 => 4}, "$test: default_fetch_filter and filter");

test 'filters';
$dbi = DBIx::Custom->new;

is($dbi->filters->{decode_utf8}->(encode_utf8('あ')),
   'あ', "$test : decode_utf8");

is($dbi->filters->{encode_utf8}->('あ'),
   encode_utf8('あ'), "$test : encode_utf8");

test 'transaction';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->dbh->begin_work;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$dbi->dbh->commit;
$result = $dbi->select(table => 'table1');
is_deeply(scalar $result->fetch_hash_all, [{key1 => 1, key2 => 2}, {key1 => 2, key2 => 3}],
          "$test : commit");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->dbh->begin_work(0);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->dbh->rollback;

$result = $dbi->select(table => 'table1');
ok(! $result->fetch_first, "$test: rollback");

test 'cache';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$source = 'select * from table1 where {= key1} and {= key2};';
$dbi->create_query($source);
is_deeply($dbi->{_cached}->{$source}, 
          {sql => "select * from table1 where key1 = ? and key2 = ?;", columns => ['key1', 'key2']}, "$test : cache");

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
    like($@, qr/\Qselect * frm table1;/, "$test : fail prepare");
    like($@, qr/\.t /, "$test: fail : not verbose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/Custom.*\.t /s, "$test : fail : verbose");
}

eval{$dbi->execute('select * from table1', no_exists => 1)};
like($@, qr/\Q"no_exists" is invalid argument/, "$test : invald SQL");

$query = $dbi->create_query('select * from table1 where {= key1}');
$dbi->dbh->disconnect;
eval{$dbi->execute($query, param => {key1 => {a => 1}})};
ok($@, "$test: execute fail");

{
    local $Carp::Verbose = 0;
    eval{$dbi->create_query('select * from table1 where {0 key1}')};
    like($@, qr/\Q.t /, "$test : caller spec : not vebose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->create_query('select * from table1 where {0 key1}')};
    like($@, qr/QueryBuilder.*\.t /s, "$test : caller spec : not vebose");
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
is_deeply($rows, [], "$test : rollback");

$dbi->begin_work;

eval {
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
};

$dbi->commit unless $@;

$result = $dbi->select(table => 'table1');
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : commit");

$dbi->dbh->{AutoCommit} = 0;
eval{ $dbi->begin_work };
ok($@, "$test : exception");
$dbi->dbh->{AutoCommit} = 1;


test 'helper';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->helper(
    one => sub { 1 }
);
$dbi->helper(
    two => sub { 2 }
);
$dbi->helper({
    twice => sub {
        my $self = shift;
        return $_[0] * 2;
    }
});

is($dbi->one, 1, "$test : first");
is($dbi->two, 2, "$test : second");
is($dbi->twice(5), 10 , "$test : second");

eval {$dbi->XXXXXX};
like($@, qr/\QCan't locate object method "XXXXXX" via "DBIx::Custom"/, "$test : not exists");

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
is_deeply($row, {key1 => 2, key2 => 6}, "$test : insert");
$result = $dbi->select(table => 'table1');
$row   = $result->fetch_hash_first;
is_deeply($row, {key1 => 6, key2 => 12}, "$test : insert");

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
is_deeply($row, {key1 => 4, key2 => 2}, "$test : update");

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
is_deeply($rows, [], "$test : delete");

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
is_deeply($rows, [{key1 => 4, key2 => 4}], "$test : select");

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
is_deeply($rows, [{key1 => 4, key2 => 2}], "$test : execute");

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
is_deeply($rows, [{key2 => 4, key3 => 18}], "$test : select : join");

$result = $dbi->select(
     table => ['table1', 'table2'],
     column => ['key2', 'key3'],
     where => {'key2' => 1, 'key3' => 2}, relation => {'table1.key1' => 'table2.key1'});

$result->filter({'key2' => 'twice'});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key2 => 4, key3 => 18}], "$test : select : join : omit");

test 'iterate_all_columns';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{2});
$dbi->execute($CREATE_TABLE->{3});

$infos = [];
$dbi->iterate_all_columns(sub {
    my ($table, $column, $cinfo) = @_;
    
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
    , $test
);

test 'table';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$table = $dbi->table('table1');
$table->insert(param => {key1 => 1, key2 => 2});
$table->insert(param => {key1 => 3, key2 => 4});
$rows = $table->select->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}],
                 "$test: select");
$rows = $table->select(where => {key2 => 2}, append => 'order by key1',
                              column => ['key1', 'key2'])->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}],
                 "$test: insert insert select");
$table->update(param => {key1 => 3}, where => {key2 => 2});
$table->update(param => {key1 => 5}, where => {key2 => 4});
$rows = $table->select(where => {key2 => 2})->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 2}],
                 "$test: update");
$table->delete(where => {key2 => 2});
$rows = $table->select->fetch_hash_all;
is_deeply($rows, [{key1 => 5, key2 => 4}], "$test: delete");
$table->update_all(param => {key1 => 3});
$rows = $table->select->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test: update_all");
$table->delete_all;
$rows = $table->select->fetch_hash_all;
is_deeply($rows, [], "$test: delete_all");

$dbi->dbh->do($CREATE_TABLE->{2});
$dbi->table('table2', ppp => sub {
    my $self = shift;
    
    return $self->name;
});
is($dbi->table('table2')->ppp, 'table2', "$test : helper");

test 'limit';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 6});
$dbi->query_builder->register_tag_processor(
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
is_deeply($rows, [{key1 => 1, key2 => 2}], $test);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 2 1}"
)->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 4},{key1 => 1, key2 => 6}], $test);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 1}"
)->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], $test);

test 'connect super';
{
    package MyDBI;
    
    use base 'DBIx::Custom';
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        return $self;
    }
    
    sub new {
        my $self = shift->SUPER::connect(@_);
        
        return $self;
    }
}

$dbi = MyDBI->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1, $test);

$dbi = MyDBI->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1, $test);


test 'end_filter';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->end_filter(key1 => sub { $_[0] * 3 }, key2 => sub { $_[0] * 5 });
$row = $result->fetch_first;
is_deeply($row, [6, 40]);

$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->end_filter(key1 => sub { $_[0] * 3 }, key2 => sub { $_[0] * 5 });
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 6, key2 => 40});


test 'empty where select';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1', where => {});
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 1, key2 => 2});

$result = $dbi->select(table => 'table1', where => [' ', {}]);
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

__END__

test 'DBIx::Custom::Where';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$where = $dbi->where
             ->clause(key1 => '{= key1}', key2 => '{= key2}')
             ->param({key1 => 1});
$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(key1 => '{= key1}', key2 => '{= key2}')
             ->param({key1 => 1, key2 => 2});
$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(key1 => '{= key1}', key2 => '{= key2}')
             ->param({});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where
             ->clause(key1 => ['{> key1}', '{< key1}'], key2 => '{= key2}')
             ->param({key1 => [0, 3], key2 => 2});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(key1 => "{= key1}" )
             ->or_clause(key2 => "{= key2}" )
             ->param({ key1 => 1, key2 => [1, 2]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->fetch_hash_all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(key1 => "{= key1}" )
             ->or_clause(key2 => "{= key2}" )
             ->param({ key1 => 1, key2 => [2]});
$result = $dbi->select(
    table => 'table1',
    where => $where
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
             ->clause(key1 => "{= key1}" )
             ->or_clause(key2 => "{= key2}" )
             ->param({key3 => 5});
$result = $dbi->select(
    table => 'table1',
    where => $where
);
};
ok($@);

$where = $dbi->where;
is("$where", '');

$where = $dbi->where->clause(key1 => 'pppp')->param({key1 => 1});
like("$where", qr/pppp/);

test 'dbi_options default';
$dbi = DBIx::Custom->new;
is_deeply($dbi->dbi_options, {});



