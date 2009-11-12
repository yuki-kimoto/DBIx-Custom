#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom::MySQL' );
}

diag( "Testing DBIx::Custom::MySQL $DBIx::Custom::MySQL::VERSION, Perl $], $^X" );
