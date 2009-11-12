#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom::Basic' );
}

diag( "Testing DBIx::Custom::Basic $DBIx::Custom::Basic::VERSION, Perl $], $^X" );
