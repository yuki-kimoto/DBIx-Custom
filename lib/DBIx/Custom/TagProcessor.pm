package DBIx::Custom::TagProcessor;

use strict;
use warnings;

use Carp 'croak';

# Carp trust relationship
push @DBIx::Custom::QueryBuilder::CARP_NOT, __PACKAGE__;

sub expand_equal_tag              { _expand_basic_tag('=',    @_) }
sub expand_greater_than_equal_tag { _expand_basic_tag('>=',   @_) }
sub expand_greater_than_tag       { _expand_basic_tag('>',    @_) }

sub expand_in_tag {
    my ($column, $count) = @_;
    
    # Check arguments
    croak qq{Column name and count of values must be specified in tag "{in }"}
      unless $column && $count && $count =~ /^\d+$/;

    # Part of statement
    my $s = "$column in (";
    for (my $i = 0; $i < $count; $i++) {
        $s .= '?, ';
    }
    $s =~ s/, $//;
    $s .= ')';
    
    # Columns
    my $columns = [];
    push @$columns, $column for (0 .. $count - 1);
    
    return [$s, $columns];
}

sub expand_insert_param_tag {
    my @columns = @_;
    
    # Insert parameters
    my $s = '(';
    $s .= "$_, " for @columns;
    $s =~ s/, $//;
    $s .= ') ';
    $s .= 'values (';
    $s .= "?, " for @columns;
    $s =~ s/, $//;
    $s .= ')';
    
    return [$s, \@columns];
}

sub expand_like_tag               { _expand_basic_tag('like', @_) }
sub expand_lower_than_equal_tag   { _expand_basic_tag('<=',   @_) }
sub expand_lower_than_tag         { _expand_basic_tag('<',    @_) }
sub expand_not_equal_tag          { _expand_basic_tag('<>',   @_) }

sub expand_placeholder_tag {
    my $column = shift;
    
    # Check arguments
    croak qq{Column name must be specified in tag "{? }"}
      unless $column;
    
    return ['?', [$column]];
}

sub expand_update_param_tag {
    my @columns = @_;
    
    # Update paramters
    my $s = 'set ';
    $s .= "$_ = ?, " for @columns;
    $s =~ s/, $//;
    
    return [$s, \@columns];
}

sub _expand_basic_tag {
    my ($name, $column) = @_;
    
    # Check arguments
    croak qq{Column name must be specified in tag "{$name }"}
      unless $column;
    
    return ["$column $name ?", [$column]];
}

1;

=head1 NAME

DBIx::Custom::TagProcessor - Tag processor

