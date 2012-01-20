use Test::More;
use strict;
use warnings;
use utf8;

use FindBin;
use DBIx::Custom;

my $dbi;
my $dsn;
my $database = "$FindBin::Bin/access2010.accdb";

$dsn = "dbi:ODBC:Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=$database";

plan skip_all => 'Microsoft access(ODBC, *.accdb(2010)) private test'
  unless -f "$FindBin::Bin/run/access2010-accdb.run"
      && eval { $dbi = DBIx::Custom->connect(dsn => $dsn); 1 };
plan 'no_plan';

my $model;
my $result;
my $row;
my $rows;

eval { $dbi->execute("drop table table1") };
eval { $dbi->execute("drop table table2") };
$dbi->execute("create table table1 (key1 varchar(255), key2 varchar(255))");
$dbi->execute("create table table2 (key1 varchar(255), key3 varchar(255))");
$model = $dbi->create_model(table => 'table1');
$model->insert({key1 => 1, key2 => 2});
$model->insert({key1 => 4, key2 => 5});
$model->insert({key1 => 6, key2 => 7});
$model->update({key2 => 3}, where => {key1 => 1});
$model->delete(where => {key1 => 6});
$rows = $model->select->all;
is_deeply($rows, [{key1 => 1, key2 => 3}, {key1 => 4, key2 => 5}]);
is($model->count, 2);
$dbi->insert({key1 => 1, key3 => 2}, table => 'table2');
$dbi->separator('-');
$row = $model->select(
  table => 'table1',
  column => {table2 => [qw/key3/]},
  join => ['left outer join table2 on table1.key1 = table2.key1']
)->one;
is_deeply($row, {"table2-key3" => 2});

