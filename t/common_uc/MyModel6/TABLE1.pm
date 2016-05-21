package MyModel6::TABLE1;

use MyModel6 -base;

has join => sub {
  [
    'left outer join TABLE2 on TABLE1.KEY1 = TABLE2.KEY1'
  ]
};
has primary_key => sub { ['KEY1'] };

1;
