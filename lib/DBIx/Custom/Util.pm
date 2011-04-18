package DBIx::Custom::Util;

use strict;
use warnings;

sub array_to_hash {
    my $array = shift;
    
    return $array if ref $array eq 'HASH';
    return unless $array;
    
    my $hash = {};
    
    for (my $i = 0; $i < @$array; $i += 2) {
        my $key = $array->[$i];
        my $f = $array->[$i + 1];
        
        if (ref $key eq 'ARRAY') {
            foreach my $k (@$key) {
                $hash->{$k} = $f;
            }
        }
        else {
            $hash->{$key} = $f;
        }
    }
    return $hash;
}

1;

=head1 NAME

DBIx::Custom::Util - Utility class

