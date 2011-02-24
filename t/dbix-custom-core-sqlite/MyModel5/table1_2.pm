package MyModel5::table1_2;

use strict;
use warnings;

use base 'MyModel5';

__PACKAGE__->attr(name => 'table1_3');
__PACKAGE__->attr(table => 'table1');

__PACKAGE__->attr('primary_key' => sub { ['key1', 'key2'] });

1;
