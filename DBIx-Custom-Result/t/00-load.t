#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom::Result' );
}

diag( "Testing DBIx::Custom::Result $DBIx::Custom::Result::VERSION, Perl $], $^X" );
