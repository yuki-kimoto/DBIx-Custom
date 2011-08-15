package MyDBI1;

use strict;
use warnings;

use base 'DBIx::Custom';

sub connect {
    my $self = shift->SUPER::connect(@_);
    
    $self->include_model(
        MyModel1 => [
            'table1',
            'table2',
            'TABLE1',
            'TABLE2'
        ]
    );
}

1;
