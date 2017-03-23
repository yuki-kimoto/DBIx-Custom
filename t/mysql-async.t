use Test::More;
use strict;
use warnings;
use utf8;

use FindBin;
use DBIx::Custom;

my $user = 'dbix_custom';
my $password = 'dbix_custom';
my $database = 'dbix_custom';
my $dsn = "dbi:mysql:database=$database";
my $args = {dsn => $dsn, user => $user, password => $password,};

plan skip_all => 'mysql private test' unless -f "$FindBin::Bin/run/mysql-async.run"
  && eval { DBIx::Custom->connect($args); 1 };
plan 'no_plan';

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

# connect
{
  my $dbi;
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
}

# async test
{
  require AnyEvent;
  my $cond = AnyEvent->condvar;
  my $dbi = DBIx::Custom->connect(
      dsn => "dbi:mysql:database=$database;",
      user => $user,
      password => $password
    );
  my $result = $dbi->execute('SELECT SLEEP(1), 3', undef,
    prepare_attr => {async => 1}, statement => 'select');

  my $dbi2 = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database;host=localhost;port=10000",
    user => $user,
    password => $password
  );
  my $result2 = $dbi2->select('key1', table => 'table1', prepare_attr => {async => 1});

  my $timer = AnyEvent->timer(
    interval => 1,
    cb => sub {
      1;
    }
  );

  my $count = 0;

  my $mysql_watcher;
  $mysql_watcher = AnyEvent->io(
    fh   => $dbi->dbh->mysql_fd,
    poll => 'r',
    cb   => sub {
      my $row = $result->fetch_one;
      is($row->[1], 3, 'before');
      $cond->send if ++$count == 2;
      undef $result;
      undef $mysql_watcher;
    }
  );

  my $mysql_watcher2= AnyEvent->io(
    fh   => $dbi2->dbh->mysql_fd,
    poll => 'r',
    cb   => sub {
      my $row = $result2->fetch_one;
      is($row->[0], 1, 'after');
      $cond->send if ++$count == 2;
      undef $result2;
    }
  );
  $cond->recv;
}
