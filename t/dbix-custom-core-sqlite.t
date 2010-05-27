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
    2 => 'create table table2 (key1 char(255), key3 char(255));'
};

my $SELECT_TMPLS = {
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
my $tmpl;
my @tmpls;
my $select_tmpl;
my $insert_tmpl;
my $update_tmpl;
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


test 'disconnect';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->disconnect;
ok(!$dbi->dbh, $test);


test 'connected';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
ok($dbi->connected, "$test : connected");

# Prepare table
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});

test 'DBIx::Custom::Result test';
$tmpl = "select key1, key2 from table1";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query);

@rows = ();
while (my $row = $result->fetch) {
    push @rows, [@$row];
}
is_deeply(\@rows, [[1, 2], [3, 4]], "$test : fetch scalar context");

$result = $dbi->execute($query);
@rows = ();
while (my @row = $result->fetch) {
    push @rows, [@row];
}
is_deeply(\@rows, [[1, 2], [3, 4]], "$test : fetch list context");

$result = $dbi->execute($query);
@rows = ();
while (my $row = $result->fetch_hash) {
    push @rows, {%$row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : fetch_hash scalar context");

$result = $dbi->execute($query);
@rows = ();
while (my %row = $result->fetch_hash) {
    push @rows, {%row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : fetch hash list context");

$result = $dbi->execute($query);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]], "$test : fetch_all scalar context");

$result = $dbi->execute($query);
@rows = $result->fetch_all;
is_deeply(\@rows, [[1, 2], [3, 4]], "$test : fetch_all list context");

$result = $dbi->execute($query);
@rows = $result->fetch_hash_all;
is_deeply($rows, [[1, 2], [3, 4]], "$test : fetch_hash_all scalar context");

$result = $dbi->execute($query);
@rows = $result->fetch_all;
is_deeply(\@rows, [[1, 2], [3, 4]], "$test : fetch_hash_all list context");


test 'Insert query return value';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$tmpl = "insert into table1 {insert key1 key2}";
$query = $dbi->create_query($tmpl);
$ret_val = $dbi->execute($query, param => {key1 => 1, key2 => 2});
ok($ret_val, $test);


test 'Direct query';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$insert_tmpl = "insert into table1 {insert key1 key2}";
$dbi->execute($insert_tmpl, param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], $test);

test 'Filter basic';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->register_filter(twice       => sub { $_[0] * 2}, 
                    three_times => sub { $_[0] * 3});

$insert_tmpl  = "insert into table1 {insert key1 key2};";
$insert_query = $dbi->create_query($insert_tmpl);
$insert_query->filter({key1 => 'twice'});
$dbi->execute($insert_query, param => {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows = $result->filter({key2 => 'three_times'})->fetch_hash_all;
is_deeply($rows, [{key1 => 2, key2 => 6}], "$test : filter fetch_filter");
$dbi->execute($DROP_TABLE->{0});

test 'Filter in';
$dbi->execute($CREATE_TABLE->{0});
$insert_tmpl  = "insert into table1 {insert key1 key2};";
$insert_query = $dbi->create_query($insert_tmpl);
$dbi->execute($insert_query, param => {key1 => 2, key2 => 4});
$select_tmpl = "select * from table1 where {in table1.key1 2} and {in table1.key2 2}";
$select_query = $dbi->create_query($select_tmpl);
$select_query->filter({'table1.key1' => 'twice'});
$result = $dbi->execute($select_query, param => {'table1.key1' => [1,5], 'table1.key2' => [2,4]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 2, key2 => 4}], "$test : filter");

test 'DBIx::Custom::SQLTemplate basic tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$tmpl = "select * from table1 where {= key1} and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, param => {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag1");

$tmpl = "select * from table1 where {<= key1} and {like key2};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, param => {key1 => 1, key2 => '%2%'});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag2");

test 'DIB::Custom::SQLTemplate in tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$tmpl = "select * from table1 where {in key1 2};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, param => {key1 => [9, 1]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic");

test 'DBIx::Custom::SQLTemplate insert tag';
$dbi->execute("delete from table1");
$insert_tmpl = 'insert into table1 {insert key1 key2 key3 key4 key5}';
$dbi->execute($insert_tmpl, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});

$result = $dbi->execute($SELECT_TMPLS->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic");

test 'DBIx::Custom::SQLTemplate update tag';
$dbi->execute("delete from table1");
$insert_tmpl = "insert into table1 {insert key1 key2 key3 key4 key5}";
$dbi->execute($insert_tmpl, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->execute($insert_tmpl, param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$update_tmpl = 'update table1 {update key1 key2 key3 key4} where {= key5}';
$dbi->execute($update_tmpl, param => {key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5});

$result = $dbi->execute($SELECT_TMPLS->{0});
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
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : basic");

$dbi->execute('delete from table1');
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_query_filter('twice');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, filter => {key1 => 'three_times'});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : filter");
$dbi->default_query_filter(undef);

$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, append => '   ');
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], 'insert append');

test 'update';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : basic");
                  
$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 12}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : update key same as search key");

$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1},
              filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 22, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : filter");


$result = $dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1}, append => '   ');

test 'update_all';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update_all(table => 'table1', param => {key2 => 10}, filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_TMPLS->{0});
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
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : basic");

$dbi->execute("delete from table1;");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->delete(table => 'table1', where => {key2 => 1}, filter => {key2 => 'twice'});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : filter");

$dbi->delete(table => 'table1', where => {key1 => 1}, append => '   ');

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1, key2 => 2});
$rows = $dbi->select(table => 'table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : delete multi key");

test 'delete error';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
eval{$dbi->delete(table => 'table1')};
like($@, qr/Key-value pairs for where clause must be specified to 'delete' second argument/,
         "$test : where key-value pairs not specified");

test 'delete_all';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete_all(table => 'table1');
$result = $dbi->execute($SELECT_TMPLS->{0});
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
    append  => "where table1.key1 = table2.key1"
)->fetch_hash_all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "$test : join");

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
$row = $result->fetch_hash_single;
is_deeply($row, {key1 => 3, key2 => 4}, "$test: default_fetch_filter and filter");

test 'filters';
$dbi = DBIx::Custom->new;

is($dbi->filters->{decode_utf8}->(encode_utf8('あ')),
   'あ', "$test : decode_utf8");

is($dbi->filters->{encode_utf8}->('あ'),
   encode_utf8('あ'), "$test : encode_utf8");

