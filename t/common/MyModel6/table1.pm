package MyModel6::table1;

use MyModel6 -base;

has join => sub {
  [
    'left outer join table2 on table1.key1 = table2.key1'
  ]
};
has primary_key => sub { ['key1'] };

1;
