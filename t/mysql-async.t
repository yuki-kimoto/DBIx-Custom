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
  my $dbi1 = DBIx::Custom->connect(
      dsn => "dbi:mysql:database=$database;",
      user => $user,
      password => $password
    );
  
  my $query1 = $dbi1->execute('SELECT SLEEP(1), 3', undef, query => 1);
  my $sth1 = $dbi1->dbh->prepare($query1->sql, {async => 1});
  $sth1->execute;
  my $result1 = $dbi1->create_result($sth1);

  my $dbi2 = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database;host=localhost;port=10000",
    user => $user,
    password => $password
  );
  my $query2 = $dbi2->select('key1', table => 'table1', query => 1);
  my $sth2 = $dbi2->dbh->prepare($query2->sql, {async => 1});
  $sth2->execute(@{$query2->bind_values});
  my $result2 = $dbi2->create_result($sth2);

  my $timer = AnyEvent->timer(
    interval => 1,
    cb => sub {
      1;
    }
  );

  my $count = 0;

  my $mysql_watcher;
  $mysql_watcher = AnyEvent->io(
    fh   => $dbi1->dbh->mysql_fd,
    poll => 'r',
    cb   => sub {
      my $row = $result1->fetch_one;
      is($row->[1], 3, 'before');
      $cond->send if ++$count == 2;
      undef $result1;
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
