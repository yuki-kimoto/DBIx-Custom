package MyModel7::main::table1;

use base 'MyModel7';

__PACKAGE__->attr(
  primary_key => sub { ['key1'] },
  join => sub {
    [
      'left outer join main.table2 on main.table1.key1 = main.table2.key1'
    ]
  },
);

1;
