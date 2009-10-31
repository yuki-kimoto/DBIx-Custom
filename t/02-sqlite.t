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

# Function for test name
my $test;
sub test {
    $test = shift;
}

# Varialbes for test
our $CREATE_TABLE = {
    0 => 'create table table1 (key1 char(255), key2 char(255));',
    1 => 'create table table1 (key1 char(255), key2 char(255), key3 char(255), key4 char(255), key5 char(255));'
};

our $SELECT_TMPL = {
    0 => 'select * from table1;'
};

my $dbi;
my $sth;
my $tmpl;
my $select_tmpl;
my $insert_tmpl;
my $update_tmpl;
my $params;
my $sql;
my $result;
my @rows;
my $rows;
my $query;
my $select_query;
my $insert_query;
my $update_query;
my $ret_val;




# Prepare table
$dbi = DBI::Custom->new(data_source => 'dbi:SQLite:dbname=:memory:');
$dbi->connect;
$dbi->do($CREATE_TABLE->{0});
$sth = $dbi->prepare("insert into table1 (key1, key2) values (?, ?);");
$sth->execute(1, 2);
$sth->execute(3, 4);


test 'DBI::Custom::Result test';
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
@rows = $result->fetch_all_hash;
is_deeply($rows, [[1, 2], [3, 4]], "$test : fetch_all_hash scalar context");


$result = $dbi->execute($query);
@rows = $result->fetch_all;
is_deeply(\@rows, [[1, 2], [3, 4]], "$test : fetch_all_hash list context");


test 'Insert query return value';
$dbi->reconnect;
$dbi->do($CREATE_TABLE->{0});
$tmpl = "insert into table1 {insert key1 key2}";
$query = $dbi->create_query($tmpl);
$ret_val = $dbi->execute($query, {key1 => 1, key2 => 2});
ok($ret_val, $test);


test 'Direct execute';
$dbi->reconnect;
$dbi->do($CREATE_TABLE->{0});
$insert_tmpl = "insert into table1 {insert key1 key2}";
$dbi->execute($insert_tmpl, {key1 => 1, key2 => 2}, sub {
    my $query = shift;
    $query->bind_filter(sub {
        my ($key, $value) = @_;
        if ($key eq 'key2') {
            return $value + 1;
        }
        return $value;
    });
});

$result = $dbi->execute($SELECT_TMPL->{0});

$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 3}], $test);


test 'Filter';
$dbi->reconnect;
$dbi->do($CREATE_TABLE->{0});

$insert_tmpl  = "insert into table1 {insert key1 key2};";
$insert_query = $dbi->create_query($insert_tmpl);
$insert_query->bind_filter(sub {
    my ($key, $value, $table, $column) = @_;
    if ($key eq 'key1' && $table eq '' && $column eq 'key1') {
        return $value * 2;
    }
    return $value;
});

$dbi->execute($insert_query, {key1 => 1, key2 => 2});

$select_query = $dbi->create_query($SELECT_TMPL->{0});
$select_query->fetch_filter(sub {
    my ($key, $value, $type, $sth, $i) = @_;
    if ($key eq 'key2' && $type =~ /char/ && $sth->can('execute') && $i == 1) {
        return $value * 3;
    }
    return $value;
});
$result = $dbi->execute($select_query);

$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 2, key2 => 6}], "$test : bind_filter fetch_filter");


$dbi->do("delete from table1;");
$insert_query->no_bind_filters('key1');
$select_query->no_fetch_filters('key2');

$dbi->execute($insert_query, {key1 => 1, key2 => 2});
$result = $dbi->execute($select_query);
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2}], 'no_fetch_filters no_bind_filters');


$dbi->reconnect;
$dbi->do($CREATE_TABLE->{0});
$insert_tmpl  = "insert into table1 {insert table1.key1 table1.key2}";

$insert_query = $dbi->create_query($insert_tmpl);
$insert_query->bind_filter(sub {
    my ($key, $value, $table, $column) = @_;
    if ($key eq 'table1.key1' && $table eq 'table1' && $column eq 'key1') {
        return $value * 3;
    }
    return $value;
});

$dbi->execute($insert_query, {table1 => {key1 => 1, key2 => 2}});

$select_query = $dbi->create_query($SELECT_TMPL->{0});
$result       = $dbi->execute($select_query);
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 3, key2 => 2}], "$test : insert with table name");

test 'DBI::Custom::SQL::Template';
$dbi->reconnect;
$dbi->do($CREATE_TABLE->{1});
$sth = $dbi->prepare("insert into table1 (key1, key2, key3, key4, key5) values (?, ?, ?, ?, ?);");
$sth->execute(1, 2, 3, 4, 5);
$sth->execute(6, 7, 8, 9, 10);

$tmpl = "select * from table1 where {= key1} and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag1");

$tmpl = "select * from table1 where {<= key1} and {like key2};";
$query = $dbi->create_query($tmpl);
$result = $dbi->execute($query, {key1 => 1, key2 => '%2%'});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test : basic tag2");


$dbi->do("delete from table1");
$insert_tmpl = 'insert into table1 {insert key1 key2 key3 key4 key5}';
$dbi->execute($insert_tmpl, {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});

$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test insert tag");

$dbi->do("delete from table1");
$dbi->execute($insert_tmpl, {'#insert' => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}});
$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test insert tag #insert");


$dbi->do("delete from table1");
$insert_tmpl = 'insert into table1 {insert table1.key1 table1.key2 table1.key3 table1.key4 table1.key5}';
$dbi->execute($insert_tmpl, {table1 => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}});

$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test insert tag with table name");

$dbi->do("delete from table1");
$dbi->execute($insert_tmpl, {'#insert' => {table1 => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}}});
$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test insert tag #insert with table name");




$dbi->do("delete from table1");
$insert_tmpl = "insert into table1 {insert key1 key2 key3 key4 key5}";
$dbi->execute($insert_tmpl, {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->execute($insert_tmpl, {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$update_tmpl = 'update table1 {update key1 key2 key3 key4} where {= key5}';
$dbi->execute($update_tmpl, {key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5});

$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5},
                  {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10}], "$test update tag");

__END__

$dbi->do("delete from table1");
$dbi->execute($update_tmpl, {'#update' => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}});
$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test update tag #update");


$dbi->do("delete from table1");
$update_tmpl = 'update table1 {update table1.key1 table1.key2 table1.key3 table1.key4 table1.key5}';
$dbi->execute($update_tmpl, {table1 => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}});

$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test update tag with table name");

$dbi->do("delete from table1");
$dbi->execute($update_tmpl, {'#update' => {table1 => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}}});
$result = $dbi->execute($SELECT_TMPL->{0});
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "$test update tag #update with table name");



__END__

# Insert values
$dbi = DBI::Custom->new;
$tmpl   = "insert into table {insert}";
$params = {insert => {key1 => 'a', key2 => 'b'}};

$dbi->filters(filter => sub {
    my ($key, $value) = @_;
    if ($key eq 'key1' && $value eq 'a') {
        return uc $value;
    }
    return $value;
});
    
($sql, @bind_values) = $dbi->_create_sql($tmpl, $params, $dbi->filters->{filter});
is($sql, "insert into table (key1, key2) values (?, ?);");
is_deeply(\@bind, ['A', 'b'], 'sql template bind' );

# Update set
$dbi = DBI::Custom->new;
$tmpl   = "update table {update}";
$params = {update => {key1 => 'a', key2 => 'b'}};

$dbi->filters(filter => sub {
    my ($key, $value) = @_;
    if ($key eq 'key1' && $value eq 'a') {
        return uc $value;
    }
    return $value;
});
    
($sql, @bind_values) = $dbi->_create_sql($tmpl, $params, $dbi->filters->{filter});
is($sql, "update table set key1 = ?, key2 = ?;");
is_deeply(\@bind, ['A', 'b'], 'sql template bind' );

$dbi->disconnnect;

# Tag 'in' is easy to wrong

