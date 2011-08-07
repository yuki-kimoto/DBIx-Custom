use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1;

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';
    sub dsn { 'dbi:SQLite:dbname=:memory:' }
    sub create_table1 { 'create table table1 (key1, key2);' }
}

require "$FindBin::Bin/common.t";
