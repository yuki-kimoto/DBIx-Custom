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

plan skip_all => 'mysql private test' unless -f "$FindBin::Bin/run/mysql-async-opt-insert.run"
  && eval { $dbi = DBIx::Custom->connect($args); 1 };
plan 'no_plan';

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

# Function for test name
sub test { print "# $_[0]\n" }

$dbi = DBIx::Custom->connect(
  dsn => "dbi:mysql:database=$database;",
  user => $user,
  password => $password
);

eval { $dbi->do('drop table table1') };
$dbi->do('create table table1 (key1 varchar(255), key2 varchar(255)) engine=InnoDB');

test 'async test';

require AnyEvent;

my $cond = AnyEvent->condvar;

$dbi->async_conf({
  prepare_attr => {async => 1},
  fh => sub { shift->dbh->mysql_fd }
});

$dbi->insert(
  {key1 => 1, key2 => 2},
  table => 'table1',
  async => sub {
    my ($dbi) = @_;
    is($dbi->last_sth->mysql_async_result, 1);
    $cond->send;
  }
);

$cond->recv;

my $rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);
