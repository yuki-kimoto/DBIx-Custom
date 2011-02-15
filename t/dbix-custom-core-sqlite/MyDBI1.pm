package MyDBI1;

use strict;
use warnings;

use base 'DBIx::Custom';

sub connect {
    my $self = shift->SUPER::connect(@_);
    
    $self->include_table(
        MyTable1 => [
            'book',
            {company => 'Company'}
        ]
    );
}

1;
