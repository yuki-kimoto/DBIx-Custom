package DBIx::Custom::Table;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

__PACKAGE__->attr(['dbi', 'name']);

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;

    # Method name
    my ($package, $mname) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

    # Method
    $self->{_methods} ||= {};
    croak qq/Can't locate object method "$mname" via "$package"/
      unless my $method = $self->{_methods}->{$mname};

    # Execute
    return $self->$method(@_);
}

sub method {
    my $self = shift;
    
    # Merge
    my $methods = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_methods} = {%{$self->{_methods} || {}}, %$methods};
    
    return $self;
}

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Methods
    my @methods = qw/insert update update_all delete delete_all select/;
    foreach my $method (@methods) {
        $self->method(
            $method => sub {
                my $self = shift;
                return $self->dbi->$method(table => $self->name, @_);
            }
        );
    }
    
    return $self;
}

sub DESTROY { }

1;

=head1 NAME

DBIx::Custom::Table - Table base class(experimental)

=head1 SYNOPSIS

use DBIx::Custom::Table;

my $table = DBIx::Custom::Table->new(name => 'books');

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<delete>

    $table->delete(where => \%where);
    
Same as C<delete()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<delete_all>

    $table->delete_all(param => $param);
    
Same as C<delete_all()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<method>

    $table->method(insert => sub {
        my $self = shift;
        
        return $self->dbi->insert(table => $self->name, @_);
    });
    
Add method to a L<DBIx::Custom::Table> object.

=head2 C<insert>

    $table->insert(param => \%param);
    
Same as C<insert()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<method>

    $table->method(
        select_complex => sub {
            my $self = shift;
            
            return $self->dbi->select($self->name, ...);
        },
        some_method => sub { ... }
    );

Define method.

=head2 C<new>

    my $table = DBIx::Custom::Table->new;

Create a L<DBIx::Custom::Table> object.

=head2 C<select>

    $table->select(param => $param);
    
Same as C<select()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<update>

    $table->update(param => \%param, where => \%where);
    
Same as C<update()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<update_all>

    $table->update_all(param => \%param);
    
Same as C<update_all()> of L<DBIx::Custom> except that
you don't have to specify table name.
