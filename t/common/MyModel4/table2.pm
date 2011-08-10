package MyModel4::table2;

use base 'MyModel4';

sub insert { shift->SUPER::insert(param => $_[0]) }
sub list { shift->select }

1;
