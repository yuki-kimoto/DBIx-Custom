package DBIx::Custom::Query;
use Object::Simple -base;

use Carp 'croak';

has 'sql';
has 'bind_values';
has 'bind_types';
has 'columns';
has 'param';

sub build {
  
}

1;
