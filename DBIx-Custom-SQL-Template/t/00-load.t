#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Custom::SQL::Template' );
}

diag( "Testing DBIx::Custom::SQL::Template $DBIx::Custom::SQL::Template::VERSION, Perl $], $^X" );
