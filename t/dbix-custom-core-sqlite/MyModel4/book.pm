package MyModel4::book;

use base 'MyModel4';

sub table { 'company' }

sub list { shift->select }

1;
