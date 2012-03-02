package MyModel8::table1;
use MyModel8 -base;

has join => sub {
  my $self = shift;
  
  my $alias = 'table2_alias';
  $alias =~ s/\./_/g;
  
  return ["left join table2 $alias on table1.key1 = $alias.key1"];
};


1;
