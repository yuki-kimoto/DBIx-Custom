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

plan skip_all => 'mysql private test' unless -f "$FindBin::Bin/run/mysql2.run"
  && eval { DBIx::Custom->connect($args); 1 };
plan 'no_plan';

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

require DBIx::Connector;

{
  my $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database",
    user => $user,
    password => $password
  );
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1 varchar(255), key2 varchar(255))');
}

# connector => 1;
{
  my $dbi = DBIx::Custom->connect(dsn => $dsn, user => $user, password => $password,
    option => {PrintError => 1}, connector => 1);
  is(ref $dbi->connector, 'DBIx::Connector');
  ok($dbi->dbh->{PrintError});
  $dbi->delete_all(table => 'table1');
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  die "Can't fork" unless defined (my $pid = fork);

  if ($pid) {
    # Parent
    my $result = $dbi->select(table => 'table1');
    is_deeply($result->fetch_hash_one, {key1 => 1, key2 => 2});
  }
  else {
    # Child
    my $result = $dbi->select(table => 'table1');
    die "Not OK" unless $result->fetch_hash_one->{key1} == 1;
  }
}

