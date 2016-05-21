package MyModel7::table1;

use MyModel7 -base;

has primary_key => sub { ['key1'] };

has join => sub {
  [
    'left outer join table2 on table1.key1 = table2.key1'
  ]
};

1;
