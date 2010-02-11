package DBIx::Custom::Column;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr([qw/column id table/]);

sub parse {
    my ($self, $key) = @_;
    
    $key ||= '';
    
    unless ($key =~ /\./) {
        $self->column($key);
        $self->table('');
        return $self;
    }
    
    my ($table, $column) = split /\./, $key;
    
    $self->column($column);
    $self->table($table);
    
    return $self;
}

1;

=head1 NAME

DBIx::Custom::Column - DBIx::Custom column

=head1 SYNOPSIS
    
    # New
    my $column = DBIx::Custom::Column->new;
    
    # Parse
    $column->parse('books.author@IDxxx');
    
    # Attributes
    my $name  = $column->name;
    my $table = $column->table;
    my $id    = $column->id;

=head1 ATTRIBUTES

=head2 id

    $column = $column->id($id);
    $id     = $column->id

=head2 name

    $column = $column->name($name);
    $name   = $column->name

=head2 table

    $column = $column->table($table);
    $table  = $column->table

=head1 METHODS

=head2 parse

    $column->parse('books.author@IDxxx');

=cut