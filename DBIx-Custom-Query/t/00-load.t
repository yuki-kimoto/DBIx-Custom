#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom::Query' );
}

diag( "Testing DBIx::Custom::Query $DBIx::Custom::Query::VERSION, Perl $], $^X" );
