package MyModel8::main::table1;
use MyModel8 -base;

has join => sub {
  my $self = shift;
  
  my $alias = 'main.table2_alias';
  $alias =~ s/\./_/g;
  
  return ["left join main.table2 $alias on main.table1.key1 = $alias.key1"];
};


1;
