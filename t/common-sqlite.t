use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/common";
$ENV{DBIX_CUSTOM_TEST_RUN} = 1;

use DBIx::Custom;
{
  package DBIx::Custom;
  no warnings 'redefine';
  
  my $table1 = 'table1';
  my $table2 = 'table2';
  my $table2_alias = 'table2_alias';
  my $table3 = 'table3';
  my $key1 = 'key1';
  my $key2 = 'key2';
  my $key3 = 'key3';
  my $key4 = 'key4';
  my $key5 = 'key5';
  my $key6 = 'key6';
  my $key7 = 'key7';
  my $key8 = 'key8';
  my $key9 = 'key9';
  my $key10 = 'key10';
  
  has table1 => $table1;
  has table2 => $table2;
  has table2_alias => $table2_alias;
  has table3 => $table3;
  has key1 => $key1;
  has key2 => $key2;
  has key3 => $key3;
  has key4 => $key4;
  has key5 => $key5;
  has key6 => $key6;
  has key7 => $key7;
  has key8 => $key8;
  has key9 => $key9;
  has key10 => $key10;
  
  my $date_typename = 'Date';
  my $datetime_typename = 'Datetime';
  
  sub date_typename { lc $date_typename }
  sub datetime_typename { lc $datetime_typename }

  my $date_datatype = 'Date';
  my $datetime_datatype = 'Datetime';
  
  sub date_datatype { lc $date_datatype }
  sub datetime_datatype { lc $datetime_datatype }

  has dsn => 'dbi:SQLite:dbname=:memory:';
  sub create_table1 { "create table $table1 ($key1 varchar, $key2 varchar)" }
  sub create_table1_2 {"create table $table1 ($key1 varchar, $key2 varchar, $key3 varchar, key4 varchar, key5 varchar)" }
  sub create_table1_type { "create table $table1 ($key1 $date_typename, $key2 $datetime_typename)" }
  
  sub create_table1_highperformance { "create table $table1 (key1 varchar, key2 varchar, key3 varchar, key4 varchar, key5 varchar, key6 varchar, key7 varchar)" }
  
  sub create_table2 { "create table $table2 ($key1 varchar, $key3 varchar)" }
  sub create_table2_2 { "create table $table2 ($key1 varchar, $key2 varchar, $key3 varchar)" }
  sub create_table3 { "create table $table3 ($key1 varchar, $key2 varchar, $key3 varchar)" }
  sub create_table_reserved { 'create table "table" ("select" varchar, "update" varchar)' }
  
}

require "$FindBin::Bin/common.t";
