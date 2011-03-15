package MyModel8::table1;

use base 'MyModel8';

__PACKAGE__->attr(join => sub { ['left join table2 as table2_alias on table1.key1 = table2_alias.key1'] });

__PACKAGE__->attr(table_alias => sub { {'table2_alias' => 'table2'} });


1;
