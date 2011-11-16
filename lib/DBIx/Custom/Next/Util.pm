package DBIx::Custom::Next::Util;

use strict;
use warnings;

use base 'Exporter';

our @EXPORT_OK = qw/_array_to_hash _subname/;

sub _array_to_hash {
    my $array = shift;
    
    return $array if ref $array eq 'HASH';
    return unless $array;
    
    my $hash = {};
    
    for (my $i = 0; $i < @$array; $i += 2) {
        my $key = $array->[$i];
        my $f = $array->[$i + 1];
        
        if (ref $key eq 'ARRAY') {
            for my $k (@$key) {
                $hash->{$k} = $f;
            }
        }
        else {
            $hash->{$key} = $f;
        }
    }
    return $hash;
}

sub _subname { '(' . (caller 1)[3] . ')' }

1;

=head1 NAME

DBIx::Custom::Next::Util - Utility class

