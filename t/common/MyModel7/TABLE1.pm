package MyModel7::TABLE1;

use base 'MyModel7';

__PACKAGE__->attr(
    primary_key => sub { ['KEY1'] },
    join => sub {
        [
            'left outer join TABLE2 on TABLE1.KEY1 = TABLE2.KEY1'
        ]
    },
);

1;
