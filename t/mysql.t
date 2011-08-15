use Test::More;
use strict;
use warnings;

use FindBin;
use DBIx::Custom;

my $dbi;

plan skip_all => 'mysql private test' unless -f "$FindBin::Bin/run/mysql.run"
  && eval { $dbi = DBIx::Custom->connect; 1 };
plan 'no_plan';

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

# user password database
our ($user, $password, $database) = qw/appuser 123456 usertest/;

require DBIx::Connector;

# Function for test name
sub test { print "# $_[0]\n" }

# Varialbes for tests
my $dbname;
my $rows;
my $result;

test 'connect';
eval {
    $dbi = DBIx::Custom->new(
        dsn => "dbi:mysql:database=$database;host=localhost;port=10000",
        user => $user,
        password => $password
    );
};
ok(!$@);

# Test memory leaks
for (1 .. 200) {
    $dbi = DBIx::Custom->connect(
        dsn => "dbi:mysql:database=$database;host=localhost;port=10000",
        user => $user,
        password => $password
    );
    $dbi->query_builder;
    $dbi->create_model(table => $table1);
    $dbi->create_model(table => $table2);
}

test 'limit';
$dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database",
    user => $user,
    password => $password
);
$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 6});
$dbi->register_tag(
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

$dbi->dbh->disconnect;
$dbi = undef;
$dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database",
    user => $user,
    password => $password
);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1, key2 => 4},
  append => "order by key2 limit 0, 1"
)->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 4}]);
$dbi->delete_all(table => 'table1');

test 'type_rule';
$dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database",
    user => $user,
    password => $password
);
eval{$dbi->execute("create table date_test (date DATE, datetime DATETIME)")};
$dbi->each_column(
    sub {
        my ($self, $table, $column, $column_info) = @_;
    }
);

$dbi->type_rule(
    into1 => {
        date=> sub {
            my $date = shift;
            $date =~ s/aaaaa//g;
            return $date;
        },
        datetime => sub {
            my $date = shift;
            $date =~ s/ccccc//g;
            return $date;
        },
    },
    from1 => {
        # DATE
        9 => sub {
                my $date = shift;
                $date .= 'bbbbb';
                return $date;
        },
        # DATETIME or TIMPESTANM
        11 => sub {
                my $date = shift;
                $date .= 'ddddd';
                return $date;
        }
    }
);

$dbi->insert(
    {
        date => 'aaaaa2010-aaaaa11-12aaaaa',
        datetime => '2010-11ccccc-12 10:ccccc55:56'
    },
    table => 'date_test'
);
is_deeply(
    $dbi->select(table => 'date_test')->fetch,
    ['2010-11-12bbbbb', '2010-11-12 10:55:56ddddd']
);

$dbi->execute("drop table date_test");

test 'dbh';
{
    my $connector = DBIx::Connector->new(
        "dbi:mysql:database=$database",
        $user,
        $password,
        DBIx::Custom->new->default_dbi_option
    );

    my $dbi = DBIx::Custom->connect(connector => $connector);
    $dbi->delete_all(table => 'table1');
    $dbi->do('insert into table1 (key1, key2) values (1, 2)');
    is($dbi->select(table => 'table1')->fetch_hash_first->{key1}, 1);
    
    $dbi = DBIx::Custom->new;
    $dbi->dbh('a');
    is($dbi->{dbh}, 'a');
}

test 'transaction';
test 'dbh';
{
    my $connector = DBIx::Connector->new(
        "dbi:mysql:database=$database",
        $user,
        $password,
        DBIx::Custom->new->default_dbi_option
    );

    my $dbi = DBIx::Custom->connect(connector => $connector);
    $dbi->delete_all(table => 'table1');
    
    $dbi->connector->txn(sub {
        $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
        $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
    });
    is_deeply($dbi->select(table => 'table1')->fetch_hash_all,
              [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

    $dbi->delete_all(table => 'table1');
    eval {
        $dbi->connector->txn(sub {
            $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
            die "Error";
            $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
        });
    };
    is_deeply($dbi->select(table => 'table1')->fetch_hash_all,
              []);
}

use DBIx::Custom;
use Scalar::Util 'blessed';
{
    my $dbi = DBIx::Custom->connect(
        user => $user,
        password => $password,
        dsn => "dbi:mysql:dbname=$database"
    );
    $dbi->connect;
    
    ok(blessed $dbi->dbh);
    can_ok($dbi->dbh, qw/prepare/);
    ok($dbi->dbh->{AutoCommit});
    ok(!$dbi->dbh->{mysql_enable_utf8});
}

{
    my $dbi = DBIx::Custom->connect(
        user => $user,
        password => $password,
        dsn => "dbi:mysql:dbname=$database",
        dbi_options => {AutoCommit => 0, mysql_enable_utf8 => 1}
    );
    $dbi->connect;
    ok(!$dbi->dbh->{AutoCommit});
    #ok($dbi->dbh->{mysql_enable_utf8});
}

test 'fork';
{
    my $connector = DBIx::Connector->new(
        "dbi:mysql:database=$database",
        $user,
        $password,
        DBIx::Custom->new->default_dbi_option
    );
    
    my $dbi = DBIx::Custom->new(connector => $connector);
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

