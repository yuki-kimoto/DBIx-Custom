package MyModel6::table1;

use base 'MyModel6';

__PACKAGE__->attr(
    join => sub {
      [
        'left outer join table2 on table1.key1 = table2.key1'
      ]
    },
    primary_key => sub { ['key1'] }
);

1;
