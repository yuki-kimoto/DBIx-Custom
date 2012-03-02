package MyModel4::dbix_custom::table1;

use MyModel4 -base;

has table => 'dbix_custom.table1';

sub list { shift->select }

1;
