package MyModel4::main::table1;

use MyModel4 -base;

has table => 'main.table1';

sub list { shift->select }

1;
