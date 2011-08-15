package MyModel8::table1;
use MyModel8 -base;

has join => sub { ['left join table2 table2_alias on table1.key1 = table2_alias.key1'] };


1;
