package DBIx::Custom::Model;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

__PACKAGE__->attr(
    ['dbi', 'table'],
    columns => sub { [] },
    primary_key => sub { [] },
    relation => sub { {} }
);

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;

    # Method name
    my ($package, $mname) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

    # Method
    $self->{_methods} ||= {};
    if (my $method = $self->{_methods}->{$mname}) {
        return $self->$method(@_)
    }
    elsif ($self->dbi->can($mname)) {
        $self->dbi->$mname(@_);
    }
    elsif ($self->dbi->dbh->can($mname)) {
        $self->dbi->dbh->$mname(@_);
    }
    else {
        croak qq/Can't locate object method "$mname" via "$package"/
    }
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
                return $self->dbi->$method(table => $self->table, @_);
            }
        );
    }
    
    return $self;
}

sub DESTROY { }

1;

=head1 NAME

DBIx::Custom::Model - Model (experimental)

=head1 SYNOPSIS

use DBIx::Custom::Table;

my $table = DBIx::Custom::Model->new(table => 'books');

=head1 ATTRIBUTES

=head2 C<(experimental) columns>

    my $columns = $model->columns;
    $model      = $model->columns(['id', 'number']);

=head2 C<dbi>

    my $dbi = $model->dbi;
    $model  = $model->dbi($dbi);

L<DBIx::Custom> object.

=head2 C<table>

    my $table = $model->table;
    $model    = $model->table('book');

Table name.
    
=head2 C<primary_key>

    my $primary_key = $model->primary_key;
    $model          = $model->primary_key(['id', 'number']);

Foreign key. This is used by C<update_at()>, C<delete_at()>,
C<select_at()>.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>,
and you can use all methods of the object set to C<dbi>.
and implements the following new ones.

=head2 C<delete>

    $table->delete(...);
    
Same as C<delete()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<delete_all>

    $table->delete_all(...);
    
Same as C<delete_all()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<method>

    $table->method(
        count => sub {
            my $self = shift;
        
            return $self->select(column => 'count(*)', @_)
                        ->fetch_first->[0];
        }
    );
    
Add method to a L<DBIx::Custom::Table> object.

=head2 C<insert>

    $table->insert(...);
    
Same as C<insert()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<new>

    my $table = DBIx::Custom::Table->new;

Create a L<DBIx::Custom::Table> object.

=head2 C<select>

    $table->select(...);
    
Same as C<select()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<update>

    $table->update(...);
    
Same as C<update()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<update_all>

    $table->update_all(param => \%param);
    
Same as C<update_all()> of L<DBIx::Custom> except that
you don't have to specify table name.
