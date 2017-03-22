package DBIx::Custom::Util;

use strict;
use warnings;
use Carp 'cluck';

use base 'Exporter';

our @EXPORT_OK = qw/_array_to_hash _subname _deprecate/;

sub _array_to_hash {
  my $array = shift;
  
  return $array if ref $array eq 'HASH';
  return unless $array;
  
  my $hash = {};
  
  for (my $i = 0; $i < @$array; $i += 2) {
    my $key = $array->[$i];
    my $f = $array->[$i + 1];
    
    if (ref $key eq 'ARRAY') {
      for my $k (@$key) { $hash->{$k} = $f }
    }
    else { $hash->{$key} = $f }
  }
  return $hash;
}

sub _subname { '(' . (caller 1)[3] . ')' }

sub _deprecate {
  my ($deprecated_version, $message) = @_;
  
  my $suppress_version = $ENV{DBIX_CUSTOM_SUPPRESS_DEPRECATION} || 0;
  
   cluck "$message (Version: $deprecated_version) (" . (caller 1)[3] . ")\n"
    if $suppress_version < $deprecated_version;
}

1;

=head1 NAME

DBIx::Custom::Util - Utility class

