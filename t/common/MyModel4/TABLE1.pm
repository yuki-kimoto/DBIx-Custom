package MyModel4::TABLE1;

use base 'MyModel4';

sub table { 'TABLE1' }

sub list { shift->select }

1;
