package MyModel6::TABLE3;

use base 'MyModel6';

__PACKAGE__->attr(filter => sub {
  [
    KEY1 => {in => sub { uc $_[0] }}
  ]
});

1;
