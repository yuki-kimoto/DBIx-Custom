package MyModel7::table1;

use base 'MyModel7';

__PACKAGE__->attr(
  primary_key => sub { ['key1'] },
  join => sub {
    [
      'left outer join table2 on table1.key1 = table2.key1'
    ]
  },
);

1;
