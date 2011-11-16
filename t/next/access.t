use Test::More;
use strict;
use warnings;
use utf8;

use FindBin;
use DBIx::Custom;

my $dbi;
my $dsn;
my $args;
my $database = "$FindBin::Bin/access.mdb";

$dsn = "dbi:ODBC:driver=Microsoft Access Driver (*.mdb);dbq=$database";

plan skip_all => 'Microsoft access(ODBC, *.mdb) private test' unless -f "$FindBin::Bin/run/access.run"
  && eval { $dbi = DBIx::Custom->connect(dsn => $dsn); 1 };
plan 'no_plan';

my $model;
my $result;
my $rows;

eval { $dbi->execute("drop table table1") };
$dbi->execute("create table table1 (key1 varchar(255), key2 varchar(255))");
$model = $dbi->create_model(table => 'table1');
$model->insert({key1 => 1, key2 => 2});
$model->insert({key1 => 4, key2 => 5});
$model->insert({key1 => 6, key2 => 7});
$model->update({key2 => 3}, where => {key1 => 1});
$model->delete(where => {key1 => 6});
$rows = $model->select->all;
is_deeply($rows, [{key1 => 1, key2 => 3}, {key1 => 4, key2 => 5}]);
is($model->count, 2);

