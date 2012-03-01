package MyModel8::main::table1;
use MyModel8 -base;

has join => sub { ['left join main.table2 main.table2_alias on main.table1.key1 = main.table2_alias.key1'] };


1;
