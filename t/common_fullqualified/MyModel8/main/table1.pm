package MyModel8::main::table1;
use MyModel8 -base;

has join => sub {
   my $self = shift;
   
   my ($q, $p) = $self->_qp;
   
   return ["left join main.table2 main_table2_alias on main.table1.key1 = main_table2_alias.key1"]
};


1;
