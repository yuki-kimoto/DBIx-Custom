package MyTable1::Company;

use strict;
use warnings;

use base 'DBIx::Custom::Table';


sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert(param => $param);
}

sub list { shift->select; }

1;
