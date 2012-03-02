package MyModel8::TABLE1;
use MyModel8 -base;

has join => sub {
  my $self = shift;
  
  my $alias = 'TABLE2_ALIAS';
  $alias =~ s/\./_/g;
  
  return ["left join TABLE2 $alias on TABLE1.KEY1 = $alias.KEY1"];
};


1;
