package MyModel4::TABLE1;

use MyModel4 -base;

has table => 'TABLE1';

sub list { shift->select }

1;
