use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1
  if -f "$FindBin::Bin/run/common-postgresql.tmp";
$ENV{DBIX_CUSTOM_SKIP_MESSAGE} = 'postgresql private test';

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';
    has dsn => "dbi:Pg:dbname=dbix_custom";
    has user  => 'dbix_custom';
    has password => 'dbix_custom';
    
    sub create_table1 { 'create table table1 (key1 varchar(255), key2 varchar(255));' }
    sub create_table1_2 {'create table table1 (key1 varchar(255), key2 varchar(255), '
     . 'key3 varchar(255), key4 varchar(255), key5 varchar(255));' }
    sub create_table1_type { 'create table table1 (key1 Date, key2 varchar(255));' }
    sub create_table2 { 'create table table2 (key1 varchar(255), key3 varchar(255));' }
    sub create_table_reserved { 'create table "table" ("select" varchar(255), "update" varchar(255))' }
}

require "$FindBin::Bin/common.t";
