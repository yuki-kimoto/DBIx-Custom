package MyModel8::table2;

use base 'MyModel8';

__PACKAGE__->attr(filter => sub {
    [
        key3 => {out => sub { $_[0] * 2}, in => sub { $_[0] * 3}, end => sub { $_[0] * 4 }}
    ]
});

1;
