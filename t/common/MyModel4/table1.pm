package MyModel4::table1;

use MyModel4 -base;

has table => 'table1';

sub list { shift->select }

1;
