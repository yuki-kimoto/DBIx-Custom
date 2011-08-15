package MyModel8::TABLE1;
use MyModel8 -base;

has join => sub { ['left join TABLE2 as TABLE2_ALIAS on TABLE1.KEY1 = TABLE2_ALIAS.KEY1'] };


1;
