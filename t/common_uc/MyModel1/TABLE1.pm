package MyModel1::TABLE1;

use DBIx::Custom::Model -base;

sub insert {
  my ($self, $param) = @_;
  
  return $self->SUPER::insert($param);
}

sub list { shift->select; }

1;
