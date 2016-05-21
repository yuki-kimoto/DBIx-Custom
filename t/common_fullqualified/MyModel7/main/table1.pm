package MyModel7::main::table1;

use MyModel7 -base;

has primary_key => sub { ['key1'] };
has join => sub {
  [
    'left outer join main.table2 on main.table1.key1 = main.table2.key1'
  ]
};

1;
