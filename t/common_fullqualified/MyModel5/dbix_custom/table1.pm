package MyModel5::dbix_custom::table1;

use MyModel5 -base;

has primary_key => sub { ['key1', 'key2'] };

1;
