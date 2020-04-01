package MyModel7::dbix_custom::table1;

use MyModel7 -base;

has primary_key => sub { ['key1'] };

has join => sub {
  [
    'left outer join dbix_custom.table2 on dbix_custom.table1.key1 = dbix_custom.table2.key1'
  ]
};

1;
