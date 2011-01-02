package DBIx::Custom::Table;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr(['dbi', 'name']);

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;

    # Method
    my ($package, $method) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

    # Helper
    $self->{_helpers} ||= {};
    croak qq/Can't locate object method "$method" via "$package"/
      unless my $helper = $self->{_helpers}->{$method};

    # Run
    return $self->$helper(@_);
}

sub helper {
    my $self = shift;
    
    # Merge
    my $helpers = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_helpers} = {%{$self->{_helpers} || {}}, %$helpers};
    
    return $self;
}

sub DESTROY { }

1;

=head1 NAME

DBIx::Custom::Model - Table base class(experimental)

=head1 SYNOPSIS

use DBIx::Custom::Table;

my $table = DBIx::Custom::Table->new(name => 'books');

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<helper>

    $table->helper(insert => sub {
        my $self = shift;
        
        return $self->dbi->insert(table => $self->name, @_);
    });
    
Add helper method to a L<DBIx::Custom::Table> object.

