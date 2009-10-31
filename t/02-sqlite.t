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
    0 => 'create table table1 (key1 char(255), key2 char(255));'
};

our $SELECT_TMPL = {
    0 => 'select key1, key2 from table1;'
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


__END__

$dbi->fetch_filter(sub {
    my ($key, $value, $type, $sth, $i) = @_;
    if ($key eq 'key1' && $value == 1 && $type =~ /char/i && $i == 0 && $sth->{TYPE}->[$i] eq $type) {
        return $value * 3;
    }
    return $value;
});

$result = $dbi->execute("select key1, key2 from table1");

$rows = $result->fetch_all;

is_deeply($rows, [[3, 2], [3, 4]], 'fetch_filter array');


$result = $dbi->execute("select key1, key2 from table1");

$rows = $result->fetch_all_hash;

is_deeply($rows, [{key1 => 3, key2 => 2}, {key1 => 3, key2 => 4}], 'fetch_filter hash');



# Expand place holer
my $dbi = DBI::Custom->new;
my $tmpl   = "select * from table where {= key1} && {<> key2} && {< k3} && {> k4} && {>= k5} && {<= k6} && {like k7}";
my $params = {key1 => 'a', key2 => 'b', k3 => 'c', k4 => 'd', k5 => 'e', k6 => 'f', k7 => 'g'};

$dbi->filters(filter => sub {
    my ($key, $value) = @_;
    if ($key eq 'key1' && $value eq 'a') {
        return uc $value;
    }
    return $value;
});

my ($sql, @bind_values) = $dbi->_create_sql($tmpl, $params, $dbi->filters->{filter});

is($sql, "select * from table where key1 = ? && key2 <> ? && k3 < ? && k4 > ? && k5 >= ? && k6 <= ? && k7 like ?;", 'sql template2');
is_deeply(\@bind, ['A', 'b', 'c', 'd', 'e', 'f', 'g'], 'sql template bind2' );

# Expand place holer upper case
my $dbi = DBI::Custom->new;
$dbi->sql_template->upper_case(1);
my $tmpl   = "select * from table where {like k7}";
my $params = {k7 => 'g'};

($sql, @bind_values) = $dbi->_create_sql($tmpl, $params);
is($sql, "select * from table where k7 LIKE ?;", 'sql template2');
is_deeply(\@bind, ['g'], 'sql template bind2' );

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

