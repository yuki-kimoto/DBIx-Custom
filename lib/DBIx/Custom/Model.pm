package DBIx::Custom::Model;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Table;

__PACKAGE__->attr(dbi => sub { DBIx::Custom->new });

sub table {
    my ($self, $table) = @_;
      
    $self->{tables}{$table}
        = DBIx::Custom::Table->new(name => $table, dbi => $self->dbi)
      unless defined $self->{tables}{$table};
    
    return $self->{tables}{$table};
}

1;

=head1 NAME

DBIx::Custom::Model - Table class(experimental)

=head1 SYNOPSIS

use MyModel;

use base 'DBIx::Custom::Model';

sub new {
    my $self = shift->SUPER::new(@_);
    
    $self->table('books')->helper(
        insert_multi => sub {
            my $self = shift;
            
            my $dbi = $self->dbi;
            
            # ...
            
        }
    );
    
    return $self;
}

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<table>

    my $table = $model->table('books');

Create a table object if not exists or get it.

