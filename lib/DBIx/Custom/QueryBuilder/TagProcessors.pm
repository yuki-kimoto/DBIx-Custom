package DBIx::Custom::QueryBuilder::TagProcessors;

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

DBIx::Custom::SQLBuilder::TagProcessors - Tag processors

=head1 Tag processors

Tag processor is function,
which receive arguments and return a part of SQL statment
and column names.
The part of SQL statment contains placeholders.
the count of placeholders must be
same as the count of column names.

    sub processor_name {
        my @args = @_;
        
        # Part of statment, which constains placeholders
        my $s;
        
        # Column names
        my $columns = [];
        
        # Do something
        # ...
        
        return [$s, $columns];
    }

=head2 C<expand_equal_tag>

    ('NAME')  ->  ['NAME = ?', ['NAME']]

=head2 C<expand_greater_than_equal_tag>

    ('NAME')  ->  ['NAME >= ?', ['NAME']]

=head2 C<expand_greater_than_tag>

    ('NAME')  ->  ['NAME > ?', ['NAME']]

=head2 C<expand_like_tag>

    ('NAME')  ->  ['NAME like ?', ['NAME']]

=head2 C<expand_lower_than_equal_tag>

    ('NAME')  ->  ['NAME <= ?', ['NAME']]

=head2 C<expand_lower_than_tag>

    ('NAME')  ->  ['NAME < ?', ['NAME']]

=head2 C<expand_in_tag>

    ('NAME', 3)  -> ['NAME in (?, ?, ?)', ['NAME', 'NAME', 'NAME']]

=head2 C<expand_insert_param_tag>

    ('NAME1', 'NAME2')
      ->  ['(NAME1, NAME2) values (?, ?, ?)', ['NAME1', 'NAME2']]

=head2 C<expand_not_equal_tag>

    ('NAME')  ->  ['NAME <> ?', ['NAME']]

=head2 C<expand_placeholder_tag>

    ('NAME')  ->  ['?', ['NAME']]

=head2 C<expand_update_param_tag>

    ('NAME1', 'NAME2')
      ->  ['set NAME1 = ?, NAME2 = ?', ['NAME1', 'NAME2']]

=cut
