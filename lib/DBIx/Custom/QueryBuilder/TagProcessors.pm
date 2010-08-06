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

sub insert {
    my @columns = @_;
    
    # Part of insert statement
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

sub update {
    my @columns = @_;
    
    # Part of update statement
    my $s = 'set ';
    $s .= "$_ = ?, " for @columns;
    $s =~ s/, $//;
    
    return [$s, \@columns];
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

=head2 C<placeholder>

    ('NAME')  ->  ['?', ['NAME']]

=head2 C<equal>

    ('NAME')  ->  ['NAME = ?', ['NAME']]

=head2 C<not_equal>

    ('NAME')  ->  ['NAME <> ?', ['NAME']]

=head2 C<greater_than>

    ('NAME')  ->  ['NAME > ?', ['NAME']]

=head2 C<lower_than>

    ('NAME')  ->  ['NAME < ?', ['NAME']]

=head2 C<greater_than_equal>

    ('NAME')  ->  ['NAME >= ?', ['NAME']]

=head2 C<lower_than_equal>

    ('NAME')  ->  ['NAME <= ?', ['NAME']]

=head2 C<like>

    ('NAME')  ->  ['NAME like ?', ['NAME']]

=head2 C<in>

    ('NAME', 3)  -> ['NAME in (?, ?, ?)', ['NAME', 'NAME', 'NAME']]

=head2 C<insert>

    ('NAME1', 'NAME2')
      ->  ['(NAME1, NAME2) values (?, ?, ?)', ['NAME1', 'NAME2']]

=head2 C<update>

    ('NAME1', 'NAME2')
      ->  ['set NAME1 = ?, NAME2 = ?', ['NAME1', 'NAME2']]

=cut
