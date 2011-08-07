package MyModel1::book;

use DBIx::Custom::Model -base;

sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert(param => $param);
}

sub list { shift->select; }

1;
