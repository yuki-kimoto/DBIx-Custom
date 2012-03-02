package MyModel6::dbix_custom::table1;

use base 'MyModel6';

__PACKAGE__->attr(
    join => sub {
      [
        'left outer join dbix_custom.table2 on dbix_custom.table1.key1 = dbix_custom.table2.key1'
      ]
    },
    primary_key => sub { ['key1'] }
);

1;
