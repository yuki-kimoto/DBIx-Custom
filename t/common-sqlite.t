use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1;

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';
    has dsn => 'dbi:SQLite:dbname=:memory:';
    sub create_table1 { 'create table table1 (key1, key2);' }
    sub create_table1_2 {'create table table1 (key1, key2, key3, key4, key5);' }
}

require "$FindBin::Bin/common.t";
