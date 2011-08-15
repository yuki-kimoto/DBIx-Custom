package MyModel4::table1;

use base 'MyModel4';

sub table { 'table1' }

sub list { shift->select }

1;
