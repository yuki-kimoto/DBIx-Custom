package DBIx::Custom::Tag;

use strict;
use warnings;

use Carp 'croak';
use DBIx::Custom::Util '_subname';

# Carp trust relationship
push @DBIx::Custom::QueryBuilder::CARP_NOT, __PACKAGE__;

sub equal              { _basic('=',  @_) }
sub greater_than_equal { _basic('>=', @_) }
sub greater_than       { _basic('>',  @_) }

sub in {
    my ($column, $count) = @_;
    
    # Check arguments
    croak qq{Column name and count of values must be specified in tag "{in }" }
        . _subname
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

sub insert_param {
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

sub like               { _basic('like', @_) }
sub lower_than_equal   { _basic('<=',   @_) }
sub lower_than         { _basic('<',    @_) }
sub not_equal          { _basic('<>',   @_) }

sub placeholder {
    my $column = shift;
    
    # Check arguments
    croak qq{Column name must be specified in tag "{? }" } . _subname
      unless $column;
    
    return ['?', [$column]];
}

sub update_param {
    my @columns = @_;
    
    # Update parameters
    my $s = 'set ';
    $s .= "$_ = ?, " for @columns;
    $s =~ s/, $//;
    
    return [$s, \@columns];
}

sub _basic {
    my ($name, $column) = @_;
    
    # Check arguments
    croak qq{Column name must be specified in tag "{$name }" } . _subname
      unless $column;
    
    return ["$column $name ?", [$column]];
}

1;

=head1 NAME

DBIx::Custom::Tag - Tag processor
