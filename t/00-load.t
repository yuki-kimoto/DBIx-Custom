#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBI::Custom' );
}

diag( "Testing DBI::Custom $DBI::Custom::VERSION, Perl $], $^X" );
