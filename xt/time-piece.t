use Test::More 'no_plan';

use strict;
use warnings;
use DBIx::Custom;

my $dbi = DBIx::Custom->connect('dbi:SQLite:dbname=:memory:');



