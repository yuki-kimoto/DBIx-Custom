package DBIx::Custom::KeyInfo;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr([qw/column id table pos/]);

sub new {
    my $self = shift;
    
    if (@_ == 1) {
        $self = $self->SUPER::new;
        $self->parse($_[0]);
        return $self;
    }
    
    return $self->SUPER::new(@_);
}

sub parse {
    my ($self, $key) = @_;
    
    # Parse
    ($key || '') =~ /^(?:(.+?)\.)?(.+?)(?:#(.+))?$/;
    $self->table($1 || '');
    $self->column($2 || '');
    $self->id($3 || '');
    
    return $self;
}

1;

=head1 NAME

DBIx::Custom::KeyInfo - DBIx::Custom column

=head1 SYNOPSIS
    
    # New
    my $key_info = DBIx::Custom::KeyInfo->new;
    
    # Parse
    $key_info->parse('books.author@IDxxx');
    
    # Attributes
    my $name  = $key_info->name;
    my $table = $key_info->table;
    my $id    = $key_info->id;

=head1 ATTRIBUTES

=head2 id

    $key_info = $key_info->id($id);
    $id     = $key_info->id

=head2 name

    $key_info = $key_info->name($name);
    $name   = $key_info->name

=head2 table

    $key_info = $key_info->table($table);
    $table  = $key_info->table

=head1 METHODS

=head2 new

    $key_info = DBIx::Custom::KeyInfo->new(\%args);
    $key_info = DBIx::Custom::KeyInfo->new(%args);
    $key_info = DBIx::Custom::KeyInfo->new('books.author@where');

=head2 parse

    $key_info->parse('books.author@IDxxx');

=cut