package MyModel4::book;

use base 'MyModel4';

sub table { 'table1' }

sub list { shift->select }

1;
