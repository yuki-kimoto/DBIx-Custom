package MyModel6::table1;

use base 'MyModel6';

__PACKAGE__->attr(
    relation => sub {
        {
            'table1.key1' => 'table2.key1'
        }
    },
    primary_key => sub { ['key1'] }
);

1;
