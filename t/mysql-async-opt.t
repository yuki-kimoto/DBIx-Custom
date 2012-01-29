use Test::More;
use strict;
use warnings;
use utf8;

use FindBin;
use DBIx::Custom;

my $dbi;
my $dsn;
my $args;
my $user = 'dbix_custom';
my $password = 'dbix_custom';
my $database = 'dbix_custom';

$dsn = "dbi:mysql:database=$database";
$args = {dsn => $dsn, user => $user, password => $password,};

plan skip_all => 'mysql private test' unless -f "$FindBin::Bin/run/mysql-async-opt.run"
  && eval { $dbi = DBIx::Custom->connect($args); 1 };
plan 'no_plan';

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

# Function for test name
sub test { print "# $_[0]\n" }

# Varialbes for tests
my $dbname;
my $row;
my $rows;
my $result;
my $result2;
my $model;
my $dbi1;
my $dbi2;
my $dbi3;
my @dbis;
my @results;

test 'connect';
eval {
  $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database;",
    user => $user,
    password => $password
  );
};
ok(!$@);

eval { $dbi->do('drop table table1') };
$dbi->do('create table table1 (key1 varchar(255), key2 varchar(255)) engine=InnoDB');
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');

test 'async test';

require AnyEvent;

my $cond = AnyEvent->condvar;

my $timer = AnyEvent->timer(
  interval => 1,
  cb => sub {
    1;
  }
);

my $count = 0;

$dbi->execute('SELECT SLEEP(1), 3', undef,
  prepare_attr => {async => 1}, select => 1,
  async => sub {
    my ($dbi, $result) = @_;
    my $row = $result->fetch_one;
    is($row->[1], 3, 'before');
    $cond->send if ++$count == 2;
  }
);

$dbi->select('key1', table => 'table1', prepare_attr => {async => 1},
  async => sub {
    my ($dbi, $result) = @_;
    my $row = $result->fetch_one;
    is($row->[0], 1, 'after1');
    $dbi->select('key1', table => 'table1', prepare_attr => {async => 1},
      async => sub {
        my ($dbi, $result) = @_;
        my $row = $result->fetch_one;
        is($row->[0], 1, 'after2');
        $cond->send if ++$count == 2;
      }
    )
  }
);

$cond->recv;
