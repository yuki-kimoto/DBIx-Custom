#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom::SQLite' );
}

diag( "Testing DBIx::Custom::SQLite $DBIx::Custom::SQLite::VERSION, Perl $], $^X" );
