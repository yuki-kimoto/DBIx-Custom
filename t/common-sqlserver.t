use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1
  if -f "$FindBin::Bin/run/common-postgresql.run";
$ENV{DBIX_CUSTOM_SKIP_MESSAGE} = 'postgresql private test';

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';

    my $date_typename = 'date';
    my $datetime_typename = 'datetime';

    sub date_typename { lc $date_typename }
    sub datetime_typename { lc $datetime_typename }

    my $date_datatype = 91;
    my $datetime_datatype = 11;

    sub date_datatype { lc $date_datatype }
    sub datetime_datatype { lc $datetime_datatype }
    
    my $dsn = "dbi:ODBC:driver={SQL Server};Server={localhost\\SQLEXPRESS};"
      . "Trusted_Connection=No;AutoTranslate=No;Database=dbix_custom;";
    has dsn => $dsn;
    has user  => 'dbix_custom';
    has password => 'dbix_custom';
    
    sub create_table1 { 'create table table1 (key1 varchar(255), key2 varchar(255));' }
    sub create_table1_2 {'create table table1 (key1 varchar(255), key2 varchar(255), '
     . 'key3 varchar(255), key4 varchar(255), key5 varchar(255));' }
    sub create_table1_type { "create table table1 (key1 $date_typename, key2 $datetime_typename);" }
    sub create_table1_highperformance { "create table table1 (ab varchar(255), bc varchar(255), "
      . "ik varchar(255), hi varchar(255), ui varchar(255), pq varchar(255), dc varchar(255));" }
    sub create_table2 { 'create table table2 (key1 varchar(255), key3 varchar(255));' }
    sub create_table2_2 { "create table table2 (key1 varchar(255), key2 varchar(255), key3 varchar(255))" }
    sub create_table3 { "create table table3 (key1 varchar(255), key2 varchar(255), key3 varchar(255))" }
    sub create_table_reserved { 'create table "table" ("select" varchar(255), "update" varchar(255))' }
}

require "$FindBin::Bin/common.t";
