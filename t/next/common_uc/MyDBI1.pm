package MyDBI1;

use strict;
use warnings;

use base 'DBIx::Custom::Next';

sub connect {
    my $self = shift->SUPER::connect(@_);
    
    $self->include_model(
        MyModel1 => [
            $self->table1,
            $self->table2
        ]
    );
}

1;
