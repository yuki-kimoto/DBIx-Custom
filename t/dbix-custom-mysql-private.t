use Test::More;
use strict;
use warnings;

# user password database
our ($USER, $PASSWORD, $DATABASE) = connect_info();

plan skip_all => 'private MySQL test' unless $USER;

plan 'no_plan';

# Function for test name
sub test { print "# $_[0]\n" }

# Functions for tests
sub connect_info {
    my $file = 'password.tmp';
    open my $fh, '<', $file
      or return;
    
    my ($user, $password, $database) = split(/\s/, (<$fh>)[0]);
    
    close $fh;
    
    return ($user, $password, $database);
}


# Varialbes for tests
my $dbi;
my $dbname;
my $rows;
my $result;

# Constant varialbes for test
my $CREATE_TABLE = {
    0 => 'create table table1 (key1 char(255), key2 char(255));'
};


use DBIx::Custom::MySQL;

test 'connect';
$dbi = DBIx::Custom::MySQL->new(user => $USER, password => $PASSWORD,
                    database => $DATABASE, host => 'localhost', port => '10000');
$dbi->connect;
like($dbi->data_source, qr/dbi:mysql:database=.*;host=localhost;port=10000;/, "created data source");
is(ref $dbi->dbh, 'DBI::db');

test 'attributes';
$dbi = DBIx::Custom::MySQL->new;
$dbi->host('a');
is($dbi->host, 'a', "host");
$dbi->port('b');
is($dbi->port, 'b', "port");

test 'limit';
$dbi = DBIx::Custom->connect(
    data_source => "dbi:mysql:database=$DATABASE",
    user => $USER,
    password => $PASSWORD
);
$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 6});
$dbi->query_builder->register_tag_processor(
    limit => sub {
        my ($count, $offset) = @_;
        
        my $s = '';
        $offset = 0 unless defined $offset;
        $s .= "limit $offset";
        $s .= ", $count";
        
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
$dbi->delete_all(table => 'table1');


test 'fork';
{
    $dbi = DBIx::Custom->connect(
        data_source => "dbi:mysql:database=$DATABASE",
        user => $USER,
        password => $PASSWORD
    );
    $dbi->delete_all(table => 'table1');
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    die "Can't fork" unless defined (my $pid = fork);

    if ($pid) {
        # Parent
        my $result = $dbi->select(table => 'table1');
        is_deeply($result->fetch_hash_first, {key1 => 1, key2 => 2});
    }
    else {
        # Child
        my $result = $dbi->select(table => 'table1');
        die "Not OK" unless $result->fetch_hash_first->{key1} == 1;
    }
}

test 'fork in transaction';
{
    $dbi = DBIx::Custom->connect(
        data_source => "dbi:mysql:database=$DATABASE",
        user => $USER,
        password => $PASSWORD
    );
    
    $dbi->begin_work;
    die "Can't fork" unless defined (my $pid = fork);
    
    if ($pid) {
        # Parent
    }
    else {
        # Child
        eval {$dbi->select(table => 'table1') };
        die "Not OK" unless $@ =~ /transaction/;
    }
}