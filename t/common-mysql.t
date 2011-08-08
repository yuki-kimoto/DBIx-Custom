use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1
  if -f "$FindBin::Bin/run/common-mysql.tmp";
$ENV{DBIX_CUSTOM_SKIP_MESSAGE} = 'mysql private test';

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';
    sub dsn { "dbi:mysql:database=dbix_custom" }
    sub user { 'dbix_custom' }
    sub password { 'dbix_custom' }
    
    sub create_table1 { 'create table table1 (key1 varchar(255), key2 varchar(255));' }
    sub create_table1_2 {'create table table1 (key1 varchar(255), key2 varchar(255), '
     . 'key3 varchar(255), key4 varchar(255), key5 varchar(255));' }
}

require "$FindBin::Bin/common.t";
