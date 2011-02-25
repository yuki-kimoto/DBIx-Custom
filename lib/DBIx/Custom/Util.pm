package DBIx::Custom::Util;

use strict;
use warnings;

sub array_filter_to_hash {
    my $array_filter = shift;
    
    return unless $array_filter;
    return $array_filter if ref $array_filter eq 'HASH';
    
    my $filter = {};
    
    for (my $i = 0; $i < @$array_filter; $i += 2) {
        my $column = $array_filter->[$i];
        my $f = $array_filter->[$i + 1];
        
        if (ref $column eq 'ARRAY') {
            foreach my $c (@$column) {
                $filter->{$c} = $f;
            }
        }
        else {
            $filter->{$column} = $f;
        }
    }
    return $filter;
}

1;

=head1 NAME

DBIx::Custom::Util - Utility class

