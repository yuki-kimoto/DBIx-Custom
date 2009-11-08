#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom' );
}

diag( "Testing DBIx::Custom $DBIx::Custom::VERSION, Perl $], $^X" );
