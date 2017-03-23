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

plan skip_all => 'mysql private test' unless -f "$FindBin::Bin/run/mysql2.run"
  && eval { $dbi = DBIx::Custom->connect($args); 1 };
plan 'no_plan';

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

require DBIx::Connector;

# connect
eval {
  $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database;host=localhost;port=10000",
    user => $user,
    password => $password
  );
};
ok(!$@);

eval { $dbi->do('drop table table1') };
$dbi->do('create table table1 (key1 varchar(255), key2 varchar(255)) engine=InnoDB');

# bulk_insert
{
  $dbi->delete_all(table => 'table1');
  $dbi->insert(
    [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}],
    table => 'table1',
    bulk_insert => 1
  );
  like($dbi->last_sql, qr/(\?.+){4}/);
  my $rows = $dbi->select(table => 'table1')->all;
  is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);
}

{
  $dbi->delete_all(table => 'table1');
  $dbi->insert(
    [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}],
    table => 'table1',
    bulk_insert => 1,
    filter => {key1 => sub { $_[0] * 2 }}
  );
  like($dbi->last_sql, qr/(\?.+){4}/);
  my $rows = $dbi->select(table => 'table1')->all;
  is_deeply($rows, [{key1 => 2, key2 => 2}, {key1 => 6, key2 => 4}]);
}

# update_or_insert
{
  $dbi->delete_all(table => 'table1');
  $dbi->update_or_insert(
    {key2 => 2},
    table => 'table1',
    id => 1,
    primary_key => 'key1',
    option => {
      select => {append => 'for update'},
      insert => {append => '    #'},
      update => {append => '     #'}
    }
  );

  my $row = $dbi->select(id => 1, table => 'table1', primary_key => 'key1')->one;
  is_deeply($row, {key1 => 1, key2 => 2}, "basic");
}

{
  $dbi->update_or_insert(
    {key2 => 3},
    table => 'table1',
    id => 1,
    primary_key => 'key1',
    option => {
      select => {append => 'for update'},
      insert => {append => '    #'},
      update => {append => '     #'}
    }
  );

  my $row = $dbi->select(id => 1, table => 'table1', primary_key => 'key1')->one;
  is_deeply($row, {key1 => 1, key2 => 3}, "basic");
}

{
  $dbi->delete_all(table => 'table1');
  my $model = $dbi->create_model(
    table => 'table1',
    primary_key => 'key1',
  );
  $model->update_or_insert(
    {key2 => 2},
    id => 1,
    option => {
      select => {append => 'for update'},
      insert => {append => '    #'},
      update => {append => '     #'}
    }
  );
  {
    my $row = $dbi->select(id => 1, table => 'table1', primary_key => 'key1')->one;
    is_deeply($row, {key1 => 1, key2 => 2}, "basic");
    $model->update_or_insert(
      {key2 => 3},
      id => 1,
      option => {
        select => {append => 'for update'},
        insert => {append => '    #'},
        update => {append => '     #'}
      }
    );
  }
  
  {
    my $row = $dbi->select(id => 1, table => 'table1', primary_key => 'key1')->one;
    is_deeply($row, {key1 => 1, key2 => 3}, "basic");
  }
}

# limit
{
  $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database",
    user => $user,
    password => $password
  );
  $dbi->delete_all(table => 'table1');
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  $dbi->insert({key1 => 1, key2 => 4}, table => 'table1');
  $dbi->insert({key1 => 1, key2 => 6}, table => 'table1');

  $dbi = DBIx::Custom->connect(
    dsn => "dbi:mysql:database=$database",
    user => $user,
    password => $password
  );
  my $rows = $dbi->select(
    table => 'table1',
    where => {key1 => 1, key2 => 4},
    append => "order by key2 limit 0, 1"
  )->fetch_hash_all;
  is_deeply($rows, [{key1 => 1, key2 => 4}]);
  $dbi->delete_all(table => 'table1');
}

# dbh
{
  my $connector = DBIx::Connector->new(
    "dbi:mysql:database=$database",
    $user,
    $password,
    DBIx::Custom->new->default_option
  );

  my $dbi = DBIx::Custom->connect(connector => $connector);
  $dbi->delete_all(table => 'table1');
  $dbi->do('insert into table1 (key1, key2) values (1, 2)');
  is($dbi->select(table => 'table1')->fetch_hash_one->{key1}, 1);
  
  $dbi = DBIx::Custom->new;
  $dbi->dbh('a');
  is($dbi->{dbh}, 'a');
}

# transaction
# dbh
{
  my $connector = DBIx::Connector->new(
    "dbi:mysql:database=$database",
    $user,
    $password,
    DBIx::Custom->new->default_option
  );

  my $dbi = DBIx::Custom->connect(connector => $connector);
  $dbi->delete_all(table => 'table1');
  
  $dbi->connector->txn(sub {
    $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
    $dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
  });
  is_deeply($dbi->select(table => 'table1')->fetch_hash_all,
    [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

  $dbi->delete_all(table => 'table1');
  eval {
    $dbi->connector->txn(sub {
      $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
      die "Error";
      $dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
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
    option => {AutoCommit => 0, mysql_enable_utf8 => 1}
  );
  $dbi->connect;
  ok(!$dbi->dbh->{AutoCommit});
  #ok($dbi->dbh->{mysql_enable_utf8});
}

# fork
{
  my $connector = DBIx::Connector->new(
    "dbi:mysql:database=$database",
    $user,
    $password,
    DBIx::Custom->new->default_option
  );
  
  my $dbi = DBIx::Custom->new(connector => $connector);
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

