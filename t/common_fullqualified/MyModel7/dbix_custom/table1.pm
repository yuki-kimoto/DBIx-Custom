package MyModel7::dbix_custom::table1;

use base 'MyModel7';

__PACKAGE__->attr(
  primary_key => sub { ['key1'] },
  join => sub {
    [
      'left outer join dbix_custom.table2 on dbix_custom.table1.key1 = dbix_custom.table2.key1'
    ]
  },
);

1;
