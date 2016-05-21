package MyModel6::main::table1;

use MyModel6 -base;

has join => sub {
  [
    'left outer join main.table2 on main.table1.key1 = main.table2.key1'
  ]
};
has primary_key => sub { ['key1'] };

1;
