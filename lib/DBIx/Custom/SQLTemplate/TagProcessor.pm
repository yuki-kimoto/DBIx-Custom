package DBIx::Custom::SQLTemplate::TagProcessors;

use strict;
use warnings;

use Carp 'croak';

sub expand_basic_tag {
    my ($tag_name, $tag_args) = @_;
    
    # Key
    my $column = $tag_args->[0];
    
    # Key is not exist
    croak("You must be pass key as argument to tag '{$tag_name }'")
      unless $column;
    
    # Expand
    return ("$column $tag_name ?", [$column]);
}

sub expand_placeholder_tag {
    my ($tag_name, $tag_args) = @_;
    
    # Key
    my $column = $tag_args->[0];
    
    # Key is not exist
    croak("You must be pass key as argument to tag '{$tag_name }'")
      unless $column;
    
    # Expand
    return ('?', [$column]);
}

sub expand_in_tag {
    my ($tag_name, $tag_args) = @_;
    my ($column, $count) = @$tag_args;
    
    # Key must be specified
    croak("You must be pass key as first argument of tag '{$tag_name }'\n" . 
          "Usage: {$tag_name \$key \$count}")
      unless $column;
    
    # Place holder count must be specified
    croak("You must be pass placeholder count as second argument of tag '{$tag_name }'\n" . 
          "Usage: {$tag_name \$key \$count}")
      if !$count || $count =~ /\D/;

    # Expand tag
    my $expand = "$column $tag_name (";
    for (my $i = 0; $i < $count; $i++) {
        $expand .= '?, ';
    }
    
    $expand =~ s/, $//;
    $expand .= ')';
    
    # Columns
    my $columns = [];
    push @$columns, $column for (0 .. $count - 1);
    
    return ($expand, $columns);
}

sub expand_insert_tag {
    my ($tag_name, $columns) = @_;
    
    # Insert key (k1, k2, k3, ..)
    my $insert_keys = '(';
    
    # placeholder (?, ?, ?, ..)
    my $place_holders = '(';
    
    foreach my $column (@$columns) {
        
        # Join insert column
        $insert_keys   .= "$column, ";
        
        # Join place holder
        $place_holders .= "?, ";
    }
    
    # Delete last ', '
    $insert_keys =~ s/, $//;
    
    # Close 
    $insert_keys .= ')';
    $place_holders =~ s/, $//;
    $place_holders .= ')';
    
    # Expand tag
    my $expand = "$insert_keys values $place_holders";
    
    return ($expand, [@$columns]);
}

sub expand_update_tag {
    my ($tag_name, $columns) = @_;
    
    # Expanded tag
    my $expand = 'set ';
    
    foreach my $column (@$columns) {

        # Join key and placeholder
        $expand .= "$column = ?, ";
    }
    
    # Delete last ', '
    $expand =~ s/, $//;
    
    return ($expand, [@$columns]);
}

1;

=head1 NAME

DBIx::Custom::SQLTemplate::TagProcessor - Tag processor

=head1 FUNCTIONS

=head2 expand_basic_tag

=head2 expand_in_tag

=head2 expand_insert_tag

=head2 expand_update_tag

