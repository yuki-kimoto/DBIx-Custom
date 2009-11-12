use Test::More;
use strict;
use warnings;
use DBI;

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1 }
        or plan skip_all => 'DBD::SQLite >= 1.00 required';

    plan 'no_plan';
    use_ok('DBIx::Custom::Result');
}

my $test;
sub test {
    $test = shift;
}

sub query {
    my ($dbh, $sql) = @_;
    my $sth = $dbh->prepare($sql);
    $sth->execute;
    return DBIx::Custom::Result->new(sth => $sth);
}

my $dbh;
my $sql;
my $sth;
my @row;
my $row;
my @rows;
my $rows;
my $result;
my $fetch_filter;
my @error;
my $error;

$dbh = DBI->connect('dbi:SQLite:dbname=:memory:', undef, undef, {PrintError => 0, RaiseError => 1});
$dbh->do("create table table1 (key1 char(255), key2 char(255));");
$dbh->do("insert into table1 (key1, key2) values ('1', '2');");
$dbh->do("insert into table1 (key1, key2) values ('3', '4');");

$sql = "select key1, key2 from table1";

test 'fetch scalar context';
$result = query($dbh, $sql);
@rows = ();
while (my $row = $result->fetch) {
    push @rows, [@$row];
}
is_deeply(\@rows, [[1, 2], [3, 4]], $test);


test 'fetch list context';
$result = query($dbh, $sql);
@rows = ();
while (my @row = $result->fetch) {
    push @rows, [@row];
}
is_deeply(\@rows, [[1, 2], [3, 4]], $test);

test 'fetch_hash scalar context';
$result = query($dbh, $sql);
@rows = ();
while (my $row = $result->fetch_hash) {
    push @rows, {%$row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], $test);


test 'fetch hash list context';
$result = query($dbh, $sql);
@rows = ();
while (my %row = $result->fetch_hash) {
    push @rows, {%row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], $test);


test 'fetch_first';
$result = query($dbh, $sql);
$row = $result->fetch_first;
is_deeply($row, [1, 2], "$test : row");
$row = $result->fetch;
ok(!$row, "$test : finished");


test 'fetch_first list context';
$result = query($dbh, $sql);
@row = $result->fetch_first;
is_deeply([@row], [1, 2], "$test : row");
@row = $result->fetch;
ok(!@row, "$test : finished");


test 'fetch_first_hash';
$result = query($dbh, $sql);
$row = $result->fetch_first_hash;
is_deeply($row, {key1 => 1, key2 => 2}, "$test : row");
$row = $result->fetch_hash;
ok(!$row, "$test : finished");


test 'fetch_first_hash list context';
$result = query($dbh, $sql);
@row = $result->fetch_first_hash;
is_deeply({@row}, {key1 => 1, key2 => 2}, "$test : row");
@row = $result->fetch_hash;
ok(!@row, "$test : finished");


test 'fetch_rows';
$dbh->do("insert into table1 (key1, key2) values ('5', '6');");
$dbh->do("insert into table1 (key1, key2) values ('7', '8');");
$dbh->do("insert into table1 (key1, key2) values ('9', '10');");
$result = query($dbh, $sql);
$rows = $result->fetch_rows(2);
is_deeply($rows, [[1, 2],
                  [3, 4]], "$test : fetch_rows first");
$rows = $result->fetch_rows(2);
is_deeply($rows, [[5, 6],
                  [7, 8]], "$test : fetch_rows secound");
$rows = $result->fetch_rows(2);
is_deeply($rows, [[9, 10]], "$test : fetch_rows third");
$rows = $result->fetch_rows(2);
ok(!$rows);


test 'fetch_rows list context';
$result = query($dbh, $sql);
@rows = $result->fetch_rows(2);
is_deeply([@rows], [[1, 2],
                  [3, 4]], "$test : fetch_rows first");
@rows = $result->fetch_rows(2);
is_deeply([@rows], [[5, 6],
                  [7, 8]], "$test : fetch_rows secound");
@rows = $result->fetch_rows(2);
is_deeply([@rows], [[9, 10]], "$test : fetch_rows third");
@rows = $result->fetch_rows(2);
ok(!@rows);


test 'fetch_rows error';
$result = query($dbh, $sql);
eval {$result->fetch_rows};
like($@, qr/Row count must be specified/, "$test : Not specified row count");


test 'fetch_rows_hash';
$result = query($dbh, $sql);
$rows = $result->fetch_rows_hash(2);
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "$test : fetch_rows first");
$rows = $result->fetch_rows_hash(2);
is_deeply($rows, [{key1 => 5, key2 => 6},
                  {key1 => 7, key2 => 8}], "$test : fetch_rows secound");
$rows = $result->fetch_rows_hash(2);
is_deeply($rows, [{key1 => 9, key2 => 10}], "$test : fetch_rows third");
$rows = $result->fetch_rows_hash(2);
ok(!$rows);


test 'fetch_rows list context';
$result = query($dbh, $sql);
@rows = $result->fetch_rows_hash(2);
is_deeply([@rows], [{key1 => 1, key2 => 2},
                    {key1 => 3, key2 => 4}], "$test : fetch_rows first");
@rows = $result->fetch_rows_hash(2);
is_deeply([@rows], [{key1 => 5, key2 => 6},
                    {key1 => 7, key2 => 8}], "$test : fetch_rows secound");
@rows = $result->fetch_rows_hash(2);
is_deeply([@rows], [{key1 => 9, key2 => 10}], "$test : fetch_rows third");
@rows = $result->fetch_rows_hash(2);
ok(!@rows);
$dbh->do("delete from table1 where key1 = 5 or key1 = 7 or key1 = 9");


test 'fetch_rows error';
$result = query($dbh, $sql);
eval {$result->fetch_rows_hash};
like($@, qr/Row count must be specified/, "$test : Not specified row count");


test 'fetch_all';
$result = query($dbh, $sql);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]], $test);

test 'fetch_all list context';
$result = query($dbh, $sql);
@rows = $result->fetch_all;
is_deeply(\@rows, [[1, 2], [3, 4]], $test);


test 'fetch_all_hash';
$result = query($dbh, $sql);
@rows = $result->fetch_all_hash;
is_deeply($rows, [[1, 2], [3, 4]], $test);


test 'fetch_all_hash list context';
$result = query($dbh, $sql);
@rows = $result->fetch_all;
is_deeply(\@rows, [[1, 2], [3, 4]], $test);


test 'fetch filter';
$fetch_filter = sub {
    my ($value, $key, $type, $sth, $i) = @_;
    if ($key eq 'key1' && $value == 1 && $type =~ /char/i && $i == 0 && $sth->{TYPE}->[$i] eq $type) {
        return $value * 3;
    }
    return $value;
};

$result = query($dbh, $sql);
$result->fetch_filter($fetch_filter);
$rows = $result->fetch_all;
is_deeply($rows, [[3, 2], [3, 4]], "$test array");

$result = query($dbh, $sql);
$result->fetch_filter($fetch_filter);
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 3, key2 => 2}, {key1 => 3, key2 => 4}], "$test hash");

$result = query($dbh, $sql);
$result->no_fetch_filters(['key1']);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]], "$test array no filter keys");

$result = query($dbh, $sql);
$result->no_fetch_filters(['key1']);
$rows = $result->fetch_all_hash;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "$test hash no filter keys");


test 'finish';
$result = query($dbh, $sql);
$result->fetch;
$result->finish;
ok(!$result->fetch, $test);

test 'error'; # Cannot real test
$result = query($dbh, $sql);
$sth = $result->sth;

@error = $result->error;
is(scalar @error, 3, "$test list context count");
is($error[0], $sth->errstr, "$test list context errstr");
is($error[1], $sth->err, "$test list context err");
is($error[2], $sth->state, "$test list context state");

$error = $result->error;
is($error, $sth->errstr, "$test scalar context");

