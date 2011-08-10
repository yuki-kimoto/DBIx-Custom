use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1;

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';

    my $date = 'Date';
    my $time = 'Time';
    my $datetime = 'Datetime';

    has dsn => 'dbi:SQLite:dbname=:memory:';
    sub quote { '""' }
    sub create_table1 { 'create table table1 (key1 varchar, key2 varchar);' }
    sub create_table1_2 {'create table table1 (key1 varchar, key2 varchar, key3 varchar, key4 varchar, key5 varchar);' }
    sub create_table1_type { 'create table table1 (key1 Date, key2 datetime);' }
    
    sub create_table1_highperformance { "create table table1 (ab varchar, bc varchar, ik varchar, hi varchar, ui varchar, pq varchar, dc varchar);" }
    
    sub create_table2 { 'create table table2 (key1 varchar, key3 varchar);' }
    sub create_table2_2 { "create table table2 (key1 varchar, key2 varchar, key3 varchar)" }
    sub create_table3 { "create table table3 (key1 varchar, key2 varchar, key3 varchar)" }
    sub create_table_reserved { 'create table "table" ("select" varchar, "update" varchar)' }
}

require "$FindBin::Bin/common.t";
