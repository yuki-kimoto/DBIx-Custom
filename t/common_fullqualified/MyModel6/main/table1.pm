package MyModel6::main::table1;

use base 'MyModel6';

__PACKAGE__->attr(
    join => sub {
      [
        'left outer join main.table2 on main.table1.key1 = main.table2.key1'
      ]
    },
    primary_key => sub { ['key1'] }
);

1;
