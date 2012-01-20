package MyModel8::TABLE2;
use MyModel8 -base;

has filter => sub {
  {
    KEY3 => {out => sub { $_[0] * 2}, in => sub { $_[0] * 3}, end => sub { $_[0] * 4 }}
  }
};

1;
