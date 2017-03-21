use Test::More;
use strict;
use warnings;
use utf8;
use Encode qw/encode_utf8 decode_utf8/;
use FindBin;
use lib "$FindBin::Bin/common";

BEGIN {
  eval { require DBD::SQLite; 1 }
    or plan skip_all => 'DBD::SQLite required';
  eval { DBD::SQLite->VERSION >= 1.25 }
    or plan skip_all => 'DBD::SQLite >= 1.25 required';

  plan 'no_plan';
  use_ok('DBIx::Custom');
}

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

use DBIx::Custom;
{
  package DBIx::Custom;
  has dsn => sub { 'dbi:SQLite:dbname=:memory:' }
}

# Constant
my $create_table1 = 'create table table1 (key1 varchar, key2 varchar);';
my $create_table_reserved = 'create table "table" ("select" varchar, "update" varchar)';
my $q = '"';
my $p = '"';

# Variables
my $dbi;
my $result;
my $row;
my $rows;
my $binary;
my $model;

# Prepare table
$dbi = DBIx::Custom->connect;


### SQLite only test
# dbi_option default
$dbi = DBIx::Custom->new;
is_deeply($dbi->option, {});


# prefix
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1 varchar, key2 varchar, primary key(key1));');
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  $dbi->insert({key1 => 1, key2 => 4}, table => 'table1', prefix => 'or replace');
  my $result = $dbi->execute('select * from table1;');
  my $rows   = $result->all;
  is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");
}

# insert ctime and mtime scalar reference
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1, key2, key3)');
  $dbi->now(\"datetime('now')");
  $dbi->insert({key1 => \"datetime('now')"}, ctime => 'key2', mtime => 'key3', table => 'table1');
  my $result = $dbi->select(table => 'table1');
  my $row = $result->one;
  is($row->{key1}, $row->{key2});
  is($row->{key1}, $row->{key3});
}
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1, key2, key3)');
  $dbi->now(\"datetime('now')");
  my $model = $dbi->create_model(ctime => 'key2', mtime => 'key3', table => 'table1');
  $model->insert({key1 => \"datetime('now')"});
  my $result = $dbi->select(table => 'table1');
  my $row = $result->one;
  is($row->{key1}, $row->{key2});
  is($row->{key1}, $row->{key3});
}

# insert ctime and mtime scalar reference
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1, key2, key3)');
  $dbi->now(\"datetime('now')");
  $dbi->insert({key1 => \"datetime('now')"}, ctime => 'key2', mtime => 'key3', table => 'table1');
  my $result = $dbi->select(table => 'table1');
  my $row = $result->one;
  is($row->{key1}, $row->{key2});
  is($row->{key1}, $row->{key3});
}

# update mtime scalar reference
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1, key2)');
  $dbi->now(\"datetime('now')");
  $dbi->insert({key1 => \"datetime('now')"}, mtime => 'key2', table => 'table1');
  my $result = $dbi->select(table => 'table1');
  my $row = $result->one;
  is($row->{key1}, $row->{key2});
}

# update_or_insert ctime and mtime
{
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1, key2, key3, key4)');
  $dbi->now(\"datetime('now')");
  my $model = $dbi->create_model(ctime => 'key2', mtime => 'key3', table => 'table1',
  primary_key => 'key4');
  $model->update_or_insert({key1 => \"datetime('now')"}, id => 1);
  my $result = $model->select(table => 'table1', id => 1);
  my $row = $result->one;
  is($row->{key1}, $row->{key2});
  is($row->{key1}, $row->{key3});
}
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1 (key1, key2)');
  $dbi->now(\"datetime('now')");
  my $model = $dbi->create_model(mtime => 'key2', table => 'table1');
  $model->insert({key1 => \"datetime('now')"});
  my $result = $dbi->select(table => 'table1');
  my $row = $result->one;
  is($row->{key1}, $row->{key2});
}

# DBIX_CUSTOM_DEBUG ok
{
  local $ENV{DBIX_CUSTOM_DEBUG} = 1;
  $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  my $error;
  local $SIG{__WARN__} = sub {
    $error = shift;
  };
  $dbi->execute('create table table1 (key1 varchar, key2 varchar, primary key(key1));');
  ok($error);
}

# quote
{
  my $dbi = DBIx::Custom->connect;
  $dbi->quote('"');
  eval { $dbi->execute("drop table ${q}table$p") };
  $dbi->execute($create_table_reserved);
  $dbi->insert({select => 1}, table => 'table');
  $dbi->delete(table => 'table', where => {select => 1});
  my $result = $dbi->execute("select * from ${q}table$p");
  my $rows   = $result->all;
  is_deeply($rows, [], "reserved word");
}

# finish statement handle
{
  {
    my $dbi = DBIx::Custom->connect;
    $dbi->execute($create_table1);
    $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
    $dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
    
    {
      my $result = $dbi->select(table => 'table1');
      {
        my $row = $result->fetch_one;
        is_deeply($row, [1, 2], "row");
      }
      {
        my $row = $result->fetch;
        ok(!$row, "finished");
      }
    }
    {
      my $result = $dbi->select(table => 'table1');
      {
        my $row = $result->fetch_hash_one;
        is_deeply($row, {key1 => 1, key2 => 2}, "row");
      }
      {
        my $row = $result->fetch_hash;
        ok(!$row, "finished");
      }
    }
    {
      $dbi->execute('create table table2 (key1, key2);');
      my $result = $dbi->select(table => 'table2');
      my $row = $result->fetch_hash_one;
      ok(!$row, "no row fetch");
    }
  }
  {
    my $dbi = DBIx::Custom->connect;
    eval { $dbi->execute('drop table table1') };
    $dbi->execute($create_table1);
    $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
    $dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
    $dbi->insert({key1 => 5, key2 => 6}, table => 'table1');
    $dbi->insert({key1 => 7, key2 => 8}, table => 'table1');
    $dbi->insert({key1 => 9, key2 => 10}, table => 'table1');
    {
      my $result = $dbi->select(table => 'table1');
      {
        my $rows = $result->fetch_multi(2);
        is_deeply($rows, [[1, 2],
                        [3, 4]], "fetch_multi first");
      }
      {
        my $rows = $result->fetch_multi(2);
        is_deeply($rows, [[5, 6],
                        [7, 8]], "fetch_multi secound");
      }
      {
        my $rows = $result->fetch_multi(2);
        is_deeply($rows, [[9, 10]], "fetch_multi third");
      }
      {
        my $rows = $result->fetch_multi(2);
        ok(!$rows);
      }
      {
        my $result = $dbi->select(table => 'table1');
        eval {$result->fetch_multi};
        like($@, qr/Row count must be specified/, "Not specified row count");
      }
    }
    {
      my $result = $dbi->select(table => 'table1');
      {
        my $rows = $result->fetch_hash_multi(2);
        is_deeply($rows, [{key1 => 1, key2 => 2},
                        {key1 => 3, key2 => 4}], "fetch_multi first");
      }
      {
        my $rows = $result->fetch_hash_multi(2);
        is_deeply($rows, [{key1 => 5, key2 => 6},
                        {key1 => 7, key2 => 8}], "fetch_multi secound");
      }
      {
        my $rows = $result->fetch_hash_multi(2);
        is_deeply($rows, [{key1 => 9, key2 => 10}], "fetch_multi third");
      }
      {
        my $rows = $result->fetch_hash_multi(2);
        ok(!$rows);
      }
    }
    {
      my $result = $dbi->select(table => 'table1');
      eval {$result->fetch_hash_multi};
      like($@, qr/Row count must be specified/, "Not specified row count");
    }
  }
}

# type option # DEPRECATED!
{
  my $dbi = DBIx::Custom->connect(
    dsn => 'dbi:SQLite:dbname=:memory:',
    option => {
      $DBD::SQLite::VERSION > 1.26 ? (sqlite_unicode => 1) : (unicode => 1)
    }
  );
  $binary = pack("I3", 1, 2, 3);
  eval { $dbi->execute('drop table table1') };
  $dbi->execute('create table table1(key1, key2)');
  $dbi->insert({key1 => $binary, key2 => 'あ'}, table => 'table1', bind_type => [key1 => DBI::SQL_BLOB]);
  {
    my $result = $dbi->select(table => 'table1');
    my $row = $result->one;
    is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
  }
  {
    my $result = $dbi->execute('select length(key1) as key1_length from table1');
    my $row = $result->one;
    is($row->{key1_length}, length $binary);
  }
}

# type_rule from
{
  my $dbi = DBIx::Custom->connect;
  $dbi->type_rule(
    from1 => {
        date => sub { uc $_[0] }
    }
  );
  $dbi->execute("create table table1 (key1 Date, key2 datetime)");
  $dbi->insert({key1 => 'a'}, table => 'table1');
  {
    my $result = $dbi->select(table => 'table1');
    is($result->fetch_one->[0], 'A');
  }
  {
    my $result = $dbi->select(table => 'table1');
    is($result->one->{key1}, 'A');
  }
}

# select limit
{
  eval { $dbi->execute('drop table table1') };
  $dbi->execute($create_table1);
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  $dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
  my $rows = $dbi->select(table => 'table1', append => "order by key1 desc limit 1")->all;
  is_deeply($rows, [{key1 => 3, key2 => 4}], "append statement");
}

# quote
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute("drop table ${q}table$p") };
  $dbi->quote('"');
  $dbi->execute($create_table_reserved);
  $dbi->insert({select => 1}, table => 'table');
  $dbi->update({update => 2}, table => 'table', where => {'table.select' => 1});
  my $result = $dbi->execute("select * from ${q}table$p");
  my $rows   = $result->all;
  is_deeply($rows, [{select => 1, update => 2}], "reserved word");
}

# limit tag
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute('drop table table1') };
  $dbi->execute($create_table1);
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  $dbi->insert({key1 => 1, key2 => 4}, table => 'table1');
  $dbi->insert({key1 => 1, key2 => 6}, table => 'table1');
}

# join function
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute("drop table table1") };
  eval { $dbi->execute("drop table table2") };
  $dbi->execute($create_table1);
  $dbi->execute("create table table2 (key1, key3)");
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  $dbi->insert({key1 => 1, key3 => 4}, table => 'table2');
  $dbi->insert({key1 => 1, key3 => 1}, table => 'table2');
  my $result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
      "left outer join table2 on coalesce(table1.key1, 0) = coalesce(table2.key1, 0) and table2.key3 > '3'"
    ]
  );
  is_deeply($result->all, [{"table2.key3" => 4}]);
}

{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute("drop table table1") };
  eval { $dbi->execute("drop table table2") };
  $dbi->execute($create_table1);
  $dbi->execute("create table table2 (key1, key3)");
  $dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
  $dbi->insert({key1 => 1, key3 => 4}, table => 'table2');
  $dbi->insert({key1 => 1, key3 => 1}, table => 'table2');
  my $result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
      "left outer join table2 on table2.key3 > '3' and coalesce(table1.key1, 0) = coalesce(table2.key1, 0)"
    ]
  );
  is_deeply($result->all, [{"table2.key3" => 4}]);
}

# select table nothing
{
  eval { $dbi->execute('drop table table1') };
  eval { $dbi->select('key1') };
  ok($@);
  my $result = $dbi->select('3');
  is($result->fetch_one->[0], 3);
}
