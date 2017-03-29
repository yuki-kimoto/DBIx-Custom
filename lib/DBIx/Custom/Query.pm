package DBIx::Custom::Query;
use Object::Simple -base;

use DBIx::Custom::Util qw/_array_to_hash _subname _deprecate/;

use Carp 'croak';

has 'sql';
has 'bind_type';
has 'columns';
has 'param';
has 'bind_values';
has 'bind_value_types';

sub build {
  my $self = shift;
  
  my $param = $self->param;
  my $columns = $self->columns;
  my $filter = $self->{_filter};
  my $type_filters = $self->{_type_filters};
  my $bind_type = $self->bind_type || {};
  
  $bind_type = _array_to_hash($bind_type) if ref $bind_type eq 'ARRAY';
  
  # Create bind values
  my @bind_values;
  my @bind_value_types;
  my %count;
  my %not_exists;
  for my $column (@$columns) {
    
    my $value = $param->{$column};
    
    # Bind value
    if(ref $value eq 'ARRAY') {
      my $i = $count{$column} || 0;
      $i += $not_exists{$column} || 0;
      my $found;
      for (my $k = $i; $i < @$value; $k++) {
        if (ref $value->[$k] eq 'DBIx::Custom::NotExists') {
            $not_exists{$column}++;
        }
        else  {
          push @bind_values, $value->[$k];
          $found = 1;
          last
        }
      }
      next unless $found;
    }
    else { push @bind_values, $value }
    
    # Filter
    if ($filter) {
      if (defined $filter->{$column}) {
        my $f = $filter->{$column};
        $bind_values[-1] = $f->($bind_values[-1]);
      }
    }
    
    # Type rule
    if ($self->{_type_rule_is_called}) {
      my $tf1 = $self->{"_into1"}->{dot}->{$column}
        || $type_filters->{1}->{$column};
      $bind_values[-1] = $tf1->($bind_values[-1]) if $tf1;
      my $tf2 = $self->{"_into2"}->{dot}->{$column}
        || $type_filters->{2}->{$column};
      $bind_values[-1] = $tf2->($bind_values[-1]) if $tf2;
    }
   
    # Bind value types
    push @bind_value_types, $bind_type->{$column};
    
    # Count up 
    $count{$column}++;
  }
  
  $self->bind_values(\@bind_values);
  $self->bind_value_types(\@bind_value_types);
}

1;
