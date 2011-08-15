package MyModel6::TABLE1;

use base 'MyModel6';

__PACKAGE__->attr(
    join => sub {
        [
            'left outer join TABLE2 on TABLE1.KEY1 = TABLE2.KEY1'
        ]
    },
    primary_key => sub { ['KEY1'] }
);

1;
