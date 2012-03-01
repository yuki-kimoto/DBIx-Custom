package MyModel5::main::table3;

use strict;
use warnings;

use base 'MyModel5';

__PACKAGE__->attr(name => 'main.table3');
__PACKAGE__->attr(table => 'main.table3');

__PACKAGE__->attr('primary_key' => sub { ['key1', 'key2'] });

1;