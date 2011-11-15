package MyModel5::TABLE3;

use strict;
use warnings;

use base 'MyModel5';

__PACKAGE__->attr(table => 'TABLE3');

__PACKAGE__->attr('primary_key' => sub { ['KEY1', 'KEY2'] });

1;