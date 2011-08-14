use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1;

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';
    
    my $date_typename = 'Date';
    my $datetime_typename = 'Datetime';
    
    sub date_typename { lc $date_typename }
    sub datetime_typename { lc $datetime_typename }

    my $date_datatype = 'Date';
    my $datetime_datatype = 'Datetime';
    
    sub date_datatype { lc $date_datatype }
    sub datetime_datatype { lc $datetime_datatype }

    has datetime_suffix => '';
    
    has dsn => 'dbi:SQLite:dbname=:memory:';
    sub create_table1 { 'create table table1 (key1 varchar, key2 varchar);' }
    sub create_table1_2 {'create table table1 (key1 varchar, key2 varchar, key3 varchar, key4 varchar, key5 varchar);' }
    sub create_table1_type { "create table table1 (key1 $date_typename, key2 $datetime_typename);" }
    
    sub create_table1_highperformance { "create table table1 (ab varchar, bc varchar, ik varchar, hi varchar, ui varchar, pq varchar, dc varchar);" }
    
    sub create_table2 { 'create table table2 (key1 varchar, key3 varchar);' }
    sub create_table2_2 { "create table table2 (key1 varchar, key2 varchar, key3 varchar)" }
    sub create_table3 { "create table table3 (key1 varchar, key2 varchar, key3 varchar)" }
    sub create_table_reserved { 'create table "table" ("select" varchar, "update" varchar)' }
    
}

require "$FindBin::Bin/common.t";
