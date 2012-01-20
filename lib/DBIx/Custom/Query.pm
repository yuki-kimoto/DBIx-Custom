package DBIx::Custom::Query;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util '_subname';

has [qw/sth statement/],
  sql => '',
  columns => sub { [] };

# DEPRECATED!
has 'default_filter';
sub filters {
  warn "DBIx::Custom::Query filters attribute method is DEPRECATED!";
  my $self = shift;
  if (@_) {
    $self->{filters} = $_[0];
    return $self;
  }
  return $self->{filters};
}

# DEPRECATED!
sub tables {
  warn "DBIx::Custom::Query tables attribute method is DEPRECATED!";
  my $self = shift;
  if (@_) {
    $self->{tables} = $_[0];
    return $self;
  }
  return $self->{tables} ||= [];
}

#DEPRECATED!
sub filter {
  Carp::carp "DBIx::Custom::Query filter method is DEPRECATED!";
  my $self = shift;
  if (@_) {
    my $filter = {};
    if (ref $_[0] eq 'HASH') {
        $filter = $_[0];
    }
    else {
      my $ef = @_ > 1 ? [@_] : $_[0];
      for (my $i = 0; $i < @$ef; $i += 2) {
        my $column = $ef->[$i];
        my $f = $ef->[$i + 1];
        if (ref $column eq 'ARRAY') {
          for my $c (@$column) { $filter->{$c} = $f }
        }
        else { $filter->{$column} = $f }
      }
    }
    for my $column (keys %$filter) {
      my $fname = $filter->{$column};
      if (exists $filter->{$column}
        && defined $fname
        && ref $fname ne 'CODE') 
      {
        my $filters = $self->{filters} || {};
        croak qq{Filter "$fname" is not registered" } . _subname
          unless exists $filters->{$fname};
        $filter->{$column} = $filters->{$fname};
      }
    }
    $self->{filter} = {%{$self->{filter} || {}}, %$filter};
    return $self;
  }
  return $self->{filter} ||= {};
}

1;

=head1 NAME

DBIx::Custom::Query - DEPRECATED!

