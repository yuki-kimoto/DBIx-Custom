package MyModel8::table1_trim;

use base 'MyModel8';

__PACKAGE__->attr(view => 'select key1, trim(key2) as key2 from table1');

1;
