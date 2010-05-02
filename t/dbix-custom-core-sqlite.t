use Test::More;
use strict;
use warnings;

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
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->connect;
$dbi->disconnect;
ok(!$dbi->dbh, $test);


test 'connected';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
ok(!$dbi->connected, "$test : not connected");
$dbi->connect;
ok($dbi->connected, "$test : connected");


test 'create_table';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$ret_val = $dbi->create_table(
                   'table1',
                   'key1 char(255)',
                   'key2 char(255)'
                 );
ok(defined $ret_val, "$test : create_table");

$dbi->insert('table1', {key1 => 1, key2 => 2});
ok(!$@, "$test : table exist");

$ret_val = $dbi->drop_table('table1');
ok(defined $ret_val, "$test : drop table");

eval{$dbi->select('table1')};
ok($@, "$test : table not exist");

# Prepare table
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->connect;
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});

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
$ret_val = $dbi->execute($query, {key1 => 1, key2 => 2});
ok($ret_val, $test);


test 'Direct query';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$insert_tmpl = "insert into table1 {insert key1 key2}";
$dbi->execute($insert_tmpl, {key1 => 1, key2 => 2});
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
$dbi->execute($insert_query, {key1 => 1, key2 => 2});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows = $result->filter({key2 => 'three_times'})->fetch_hash_all;
is_deeply($rows, [{key1 => 2, key2 => 6}], "$test : filter fetch_filter");
$dbi->execute($DROP_TABLE->{0});

test 'Filter in';
$dbi->execute($CREATE_TABLE->{0});
$insert_tmpl  = "insert into table1 {insert key1 key2};";
$insert_query = $dbi->create_query($insert_tmpl);
$dbi->execute($insert_query, {key1 => 2, key2 => 4});
$select_tmpl = "select * from table1 where {in table1.key1 2} and {in table1.key2 2}";
$select_query = $dbi->create_query($select_tmpl);
$select_query->filter({'table1.key1' => 'twice'});
$result = $dbi->execute($select_query, {'table1.key1' => [1,5], 'table1.key2' => [2,4]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 2, key2 => 4}], "$test : filter");

test 'DBIx::Custom::SQLTemplate basic tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert('table1', {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert('table1', {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$tmpl = "select * from table1 where {= key1} and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag1");

$tmpl = "select * from table1 where {<= key1} and {like key2};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, {key1 => 1, key2 => '%2%'});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag2");

test 'DIB::Custom::SQLTemplate in tag';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert('table1', {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert('table1', {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$tmpl = "select * from table1 where {in key1 2};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, {key1 => [9, 1]});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic");

test 'DBIx::Custom::SQLTemplate insert tag';
$dbi->execute("delete from table1");
$insert_tmpl = 'insert into table1 {insert key1 key2 key3 key4 key5}';
$dbi->execute($insert_tmpl, {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});

$result = $dbi->execute($SELECT_TMPLS->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic");

test 'DBIx::Custom::SQLTemplate update tag';
$dbi->execute("delete from table1");
$insert_tmpl = "insert into table1 {insert key1 key2 key3 key4 key5}";
$dbi->execute($insert_tmpl, {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->execute($insert_tmpl, {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$update_tmpl = 'update table1 {update key1 key2 key3 key4} where {= key5}';
$dbi->execute($update_tmpl, {key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5});

$result = $dbi->execute($SELECT_TMPLS->{0});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5},
                  {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10}], "$test : basic");

test 'transaction';
$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->run_transaction(sub {
    $insert_tmpl = 'insert into table1 {insert key1 key2}';
    $dbi->execute($insert_tmpl, {key1 => 1, key2 => 2});
    $dbi->execute($insert_tmpl, {key1 => 3, key2 => 4});
});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : commit");

$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->dbh->{RaiseError} = 0;
eval{
    $dbi->run_transaction(sub {
        $insert_tmpl = 'insert into table1 {insert key1 key2}';
        $dbi->execute($insert_tmpl, {key1 => 1, key2 => 2});
        die "Fatal Error";
        $dbi->execute($insert_tmpl, {key1 => 3, key2 => 4});
    })
};
like($@, qr/Fatal Error.*Rollback is success/ms, "$test : Rollback success message");
ok(!$dbi->dbh->{RaiseError}, "$test : restore RaiseError value");
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [], "$test : rollback");



test 'Error case';
$dbi = DBIx::Custom->new;
eval{$dbi->run_transaction};
like($@, qr/Not yet connect to database/, "$test : Yet Connected");

$dbi = DBIx::Custom->new(data_source => 'dbi:SQLit');
eval{$dbi->connect;};
ok($@, "$test : connect error");

$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->connect;
$dbi->dbh->{AutoCommit} = 0;
eval{$dbi->run_transaction};
like($@, qr/AutoCommit must be true before transaction start/,
         "$test : transaction auto commit is false");

$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$sql = 'laksjdf';
eval{$dbi->execute($sql, qw/1 2 3/)};
like($@, qr/$sql/, "$test : query fail");

$dbi = DBIx::Custom->new($NEW_ARGS->{0});
eval{$dbi->create_query("{p }")};
ok($@, "$test : create_query invalid SQL template");

test 'insert';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test : basic");

$dbi->execute('delete from table1');
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_query_filter('twice');
$dbi->insert('table1', {key1 => 1, key2 => 2}, {filter => {key1 => 'three_times'}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : filter");
$dbi->default_query_filter(undef);

$dbi->execute($DROP_TABLE->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2}, {append => '   '});
$rows = $dbi->select('table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], 'insert append');


test 'insert error';
eval{$dbi->insert('table1')};
like($@, qr/Key-value pairs for insert must be specified to 'insert' second argument/, "$test : insert key-value not specifed");

test 'update';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert('table1', {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert('table1', {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update('table1', {key2 => 11}, {where => {key1 => 1}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : basic");
                  
$dbi->execute("delete from table1");
$dbi->insert('table1', {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert('table1', {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update('table1', {key2 => 12}, {where => {key2 => 2, key3 => 3}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : update key same as search key");

$dbi->execute("delete from table1");
$dbi->insert('table1', {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert('table1', {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update('table1', {key2 => 11}, {where => {key1 => 1},
              filter => {key2 => 'twice'}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 22, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "$test : filter");


$result = $dbi->update('table1', {key2 => 11}, {where => {key1 => 1}, append => '   '});

test 'update error';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
eval{$dbi->update('table1')};
like($@, qr/Key-value pairs for update must be specified to 'update' second argument/,
         "$test : update key-value pairs not specified");

eval{$dbi->update('table1', {key2 => 1})};
like($@, qr/Key-value pairs for where clause must be specified to 'update' third argument/,
         "$test : where key-value pairs not specified");

test 'update_all';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{1});
$dbi->insert('table1', {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert('table1', {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update_all('table1', {key2 => 10}, {filter => {key2 => 'twice'}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 20, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 20, key3 => 8, key4 => 9, key5 => 10}],
                  "$test : filter");


test 'delete';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});
$dbi->delete('table1', {where => {key1 => 1}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : basic");

$dbi->execute("delete from table1;");
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->delete('table1', {where => {key2 => 1}, filter => {key2 => 'twice'}});
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : filter");

$dbi->delete('table1', {where => {key1 => 1}, append => '   '});

$dbi->delete_all('table1');
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});
$dbi->delete('table1', {where => {key1 => 1, key2 => 2}});
$rows = $dbi->select('table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : delete multi key");


test 'delete error';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
eval{$dbi->delete('table1')};
like($@, qr/Key-value pairs for where clause must be specified to 'delete' second argument/,
         "$test : where key-value pairs not specified");

test 'delete_all';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});
$dbi->delete_all('table1');
$result = $dbi->execute($SELECT_TMPLS->{0});
$rows   = $result->fetch_hash_all;
is_deeply($rows, [], "$test : basic");


test 'select';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
$dbi->insert('table1', {key1 => 3, key2 => 4});
$rows = $dbi->select('table1')->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "$test : table");

$rows = $dbi->select('table1', {columns => ['key1']})->fetch_hash_all;
is_deeply($rows, [{key1 => 1}, {key1 => 3}], "$test : table and columns and where key");

$rows = $dbi->select('table1', {where => {key1 => 1}})->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "$test : table and columns and where key");

$rows = $dbi->select('table1', {columns => ['key1'], where => {key1 => 3}})->fetch_hash_all;
is_deeply($rows, [{key1 => 3}], "$test : table and columns and where key");

$rows = $dbi->select('table1', {append => "order by key1 desc limit 1"})->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "$test : append statement");

$dbi->register_filter(decrement => sub { $_[0] - 1 });
$rows = $dbi->select('table1', {where => {key1 => 2}, filter => {key1 => 'decrement'}})
            ->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "$test : filter");

$dbi->execute($CREATE_TABLE->{2});
$dbi->insert('table2', {key1 => 1, key3 => 5});
$rows = $dbi->select([qw/table1 table2/],
                      {
                         columns => ['table1.key1 as table1_key1', 'table2.key1 as table2_key1', 'key2', 'key3'],
                         where   => {'table1.key2' => 2},
                         append  => "where table1.key1 = table2.key1"
                      }
                    )->fetch_hash_all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "$test : join");

test 'Cache';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
DBIx::Custom->query_cache_max(2);
$dbi->execute($CREATE_TABLE->{0});
delete $DBIx::Custom::CLASS_ATTRS->{_query_caches};
delete $DBIx::Custom::CLASS_ATTRS->{_query_cache_keys};
$tmpls[0] = "insert into table1 {insert key1 key2}";
$queries[0] = $dbi->create_query($tmpls[0]);
is(DBIx::Custom->_query_caches->{$tmpls[0]}{sql}, $queries[0]->sql, "$test : sql first");
is(DBIx::Custom->_query_caches->{$tmpls[0]}{columns}, $queries[0]->columns, "$test : columns first");
is_deeply(DBIx::Custom->_query_cache_keys, [@tmpls], "$test : cache key first");

$tmpls[1] = "select * from table1";
$queries[1] = $dbi->create_query($tmpls[1]);
is(DBIx::Custom->_query_caches->{$tmpls[0]}{sql}, $queries[0]->sql, "$test : sql first");
is(DBIx::Custom->_query_caches->{$tmpls[0]}{columns}, $queries[0]->columns, "$test : columns first");
is(DBIx::Custom->_query_caches->{$tmpls[1]}{sql}, $queries[1]->sql, "$test : sql second");
is(DBIx::Custom->_query_caches->{$tmpls[1]}{columns}, $queries[1]->columns, "$test : columns second");
is_deeply(DBIx::Custom->_query_cache_keys, [@tmpls], "$test : cache key second");

$tmpls[2] = "select key1, key2 from table1";
$queries[2] = $dbi->create_query($tmpls[2]);
ok(!exists DBIx::Custom->_query_caches->{$tmpls[0]}, "$test : cache overflow deleted key");
is(DBIx::Custom->_query_caches->{$tmpls[1]}{sql}, $queries[1]->sql, "$test : sql cache overflow deleted key");
is(DBIx::Custom->_query_caches->{$tmpls[1]}{columns}, $queries[1]->columns, "$test : columns cache overflow deleted key");
is(DBIx::Custom->_query_caches->{$tmpls[2]}{sql}, $queries[2]->sql, "$test : sql cache overflow deleted key");
is(DBIx::Custom->_query_caches->{$tmpls[2]}{columns}, $queries[2]->columns, "$test : columns cache overflow deleted key");
is_deeply(DBIx::Custom->_query_cache_keys, [@tmpls[1, 2]], "$test : cache key third");

$queries[1] = $dbi->create_query($tmpls[1]);
ok(!exists DBIx::Custom->_query_caches->{$tmpls[0]}, "$test : cache overflow deleted key");
is(DBIx::Custom->_query_caches->{$tmpls[1]}{sql}, $queries[1]->sql, "$test : sql cache overflow deleted key");
is_deeply(DBIx::Custom->_query_caches->{$tmpls[1]}{columns}, $queries[1]->columns, "$test : columns cache overflow deleted key");
is(DBIx::Custom->_query_caches->{$tmpls[2]}{sql}, $queries[2]->sql, "$test : sql cache overflow deleted key");
is_deeply(DBIx::Custom->_query_caches->{$tmpls[2]}{columns}, $queries[2]->columns, "$test : columns cache overflow deleted key");
is_deeply(DBIx::Custom->_query_cache_keys, [@tmpls[1, 2]], "$test : cache key third");

$query = $dbi->create_query($tmpls[0]);
$query->filter('aaa');
$query = $dbi->create_query($tmpls[0]);
ok(!$query->filter, "$test : only cached sql and columns");
$query->filter('bbb');
$query = $dbi->create_query($tmpls[0]);
ok(!$query->filter, "$test : only cached sql and columns");

test 'fetch filter';
$dbi = DBIx::Custom->new($NEW_ARGS->{0});
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_fetch_filter('twice');
$dbi->execute($CREATE_TABLE->{0});
$dbi->insert('table1', {key1 => 1, key2 => 2});
$result = $dbi->select('table1');
$result->filter({key1 => 'three_times'});
$row = $result->fetch_hash_single;
is_deeply($row, {key1 => 3, key2 => 4}, "$test: default_fetch_filter and filter");

