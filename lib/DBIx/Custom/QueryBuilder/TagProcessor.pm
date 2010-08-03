package DBIx::Custom::QueryBuilder::TagProcessors;

use strict;
use warnings;

use Carp 'croak';

sub expand_basic_tag {
    my ($name, $args) = @_;
    
    # Column
    my $column = $args->[0];
    
    # Check arguments
    croak qq{Column must be specified in tag "{$name }"}
      unless $column;
    
    return ["$column $name ?", [$column]];
}

sub expand_equal_tag              { expand_basic_tag('=', @_) }
sub expand_not_equal_tag          { expand_basic_tag('<>', @_) }
sub expand_greater_than_tag       { expand_basic_tag('>', @_) }
sub expand_lower_than_tag         { expand_basic_tag('<', @_) }
sub expand_greater_than_equal_tag { expand_basic_tag('>=', @_) }
sub expand_lower_than_equal_tag   { expand_basic_tag('<=', @_) }
sub expand_like_tag               { expand_basic_tag('like', @_) }

sub expand_placeholder_tag {
    my $tag_args = shift;
    
    # Column
    my $column = $tag_args->[0];
    
    # Check arguments
    croak qq{Column must be specified in tag "{? }"}
      unless $column;
    
    return ['?', [$column]];
}

sub expand_in_tag {
    my ($column, $count) = @{$_[0]};
    
    # Check arguments
    croak qq{Column and count of values must be specified in tag "{in }"}
      unless $column && $count && $count =~ /^\d+$/;

    # Expand
    my $expand = "$column in (";
    for (my $i = 0; $i < $count; $i++) {
        $expand .= '?, ';
    }
    $expand =~ s/, $//;
    $expand .= ')';
    
    # Columns
    my $columns = [];
    push @$columns, $column for (0 .. $count - 1);
    
    return [$expand, $columns];
}

sub expand_insert_tag {
    my $columns = shift;
    
    # Insert
    my $expand = '(';
    $expand .= "$_, " for @$columns;
    $expand =~ s/, $//;
    $expand .= ') ';
    $expand .= 'values (';
    $expand .= "?, " for @$columns;
    $expand =~ s/, $//;
    $expand .= ')';
    
    return [$expand, [@$columns]];
}

sub expand_update_tag {
    my $columns = shift;
    
    # Update
    my $expand = 'set ';
    $expand .= "$_ = ?, " for @$columns;
    $expand =~ s/, $//;
    
    return [$expand, [@$columns]];
}

1;

=head1 NAME

DBIx::Custom::SQLBuilder::TagProcessors - Tag processor

=head1 FUNCTIONS

=head2 C<expand_basic_tag>

=head2 C<expand_equal_tag>

=head2 C<expand_not_equal_tag>

=head2 C<expand_greater_than_tag>

=head2 C<expand_lower_than_tag>

=head2 C<expand_greater_than_equal_tag>

=head2 C<expand_lower_than_equal_tag>

=head2 C<expand_like_tag>

=head2 C<expand_in_tag>

=head2 C<expand_insert_tag>

=head2 C<expand_update_tag>

