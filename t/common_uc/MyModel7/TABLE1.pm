package MyModel7::TABLE1;

use MyModel7 -base;

has primary_key => sub { ['KEY1'] };

has join => sub {
  [
    'left outer join TABLE2 on TABLE1.KEY1 = TABLE2.KEY1'
  ]
};

1;
