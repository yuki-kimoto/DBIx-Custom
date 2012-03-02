package MyModel8::dbix_custom::table1;
use MyModel8 -base;

has join => sub {
  my $self = shift;
  
  my $alias = 'dbix_custom.table2_alias';
  $alias =~ s/\./_/g;
  
  return ["left join dbix_custom.table2 $alias on dbix_custom.table1.key1 = $alias.key1"];
};


1;
