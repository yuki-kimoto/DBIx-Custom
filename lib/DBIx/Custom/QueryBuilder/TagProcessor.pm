package DBIx::Custom::QueryBuilder::TagProcessors;

use strict;
use warnings;

use Carp 'croak';

sub _basic {
    my ($name, $column) = @_;
    
    # Check arguments
    croak qq{Column must be specified in tag "{$name }"}
      unless $column;
    
    return ["$column $name ?", [$column]];
}

sub equal              { _basic('=',    @_) }
sub not_equal          { _basic('<>',   @_) }
sub greater_than       { _basic('>',    @_) }
sub lower_than         { _basic('<',    @_) }
sub greater_than_equal { _basic('>=',   @_) }
sub lower_than_equal   { _basic('<=',   @_) }
sub like               { _basic('like', @_) }

sub placeholder {
    my $column = shift;
    
    # Check arguments
    croak qq{Column must be specified in tag "{? }"}
      unless $column;
    
    return ['?', [$column]];
}

sub in {
    my ($column, $count) = @_;
    
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

sub insert {
    my @columns = @_;
    
    # Insert
    my $expand = '(';
    $expand .= "$_, " for @columns;
    $expand =~ s/, $//;
    $expand .= ') ';
    $expand .= 'values (';
    $expand .= "?, " for @columns;
    $expand =~ s/, $//;
    $expand .= ')';
    
    return [$expand, \@columns];
}

sub update {
    my @columns = @_;
    
    # Update
    my $expand = 'set ';
    $expand .= "$_ = ?, " for @columns;
    $expand =~ s/, $//;
    
    return [$expand, \@columns];
}

1;

=head1 NAME

DBIx::Custom::SQLBuilder::TagProcessors - Tag processor

=head1 FUNCTIONS

=head2 C<placeholder>

=head2 C<equal>

=head2 C<not_equal>

=head2 C<greater_than>

=head2 C<lower_than>

=head2 C<greater_than_equal>

=head2 C<lower_than_equal>

=head2 C<like>

=head2 C<in>

=head2 C<insert>

=head2 C<update>

