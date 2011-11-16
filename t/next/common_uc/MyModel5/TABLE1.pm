package MyModel5::TABLE1;

use MyModel5 -base;

has primary_key => sub { ['KEY1', 'KEY2'] };

1;
