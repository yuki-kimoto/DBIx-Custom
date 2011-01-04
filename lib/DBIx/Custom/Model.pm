package DBIx::Custom::Model;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr(dbi => sub { DBIx::Custom->new });
__PACKAGE__->attr(table_class => 'DBIx::Custom::Table');
__PACKAGE__->attr(tables => sub { {} });

sub table {
    my ($self, $name) = @_;
    
    # Table class
    my $table_class = $self->table_class;
    croak qq{Invalid table class name "$table_class"}
      unless $table_class =~ /^[\w:]+$/;
    unless ($table_class->can('isa')) {
        eval "require $table_class";
        croak $@ if $@;
    }
    # Create table
    $self->tables->{$name}
        = $table_class->new(name => $name, dbi => $self->dbi, model => $self)
      unless defined $self->tables->{$name};
    
    return $self->{tables}{$name};
}

1;

=head1 NAME

DBIx::Custom::Model - Table class(experimental)

=head1 SYNOPSIS

use MyModel;

use base 'DBIx::Custom::Model';

sub new {
    my $self = shift->SUPER::new(@_);
    
    my $dbi = DBIx::Custom->connect(...);
    
    $self->dbi($dbi);
    
    $self->table('book')->helper(
        insert => sub {
            my $self = shift;
            
            return $self->dbi->insert(table => $self->name, @_);
        }
    );
    
    return $self;
}

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<table>

    my $table = $model->table('book');

Get a L<DBIx::Custom::Table>, or create a L<DBIx::Custom::Table> object if not exists.

