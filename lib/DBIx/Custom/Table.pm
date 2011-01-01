package DBIx::Custom::Table;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr(['dbi', 'name']);

sub new {
    my $self = shift->SUPER::new(@_);
    
    my $insert = sub {
        my $self = shift;
        
        return $self->dbi->insert(table => $self->name, param => shift);
    };
    
    my $update = sub {
        my $self = shift;
        
        return $self->dbi->update(table => $self->name, param => shift,
                                  where => shift);
    };
    
    my $update_all = sub {
        my $self = shift;
        
        return $self->dbi->update_all(table => $self->name, param => shift);
    };
    
    my $delete = sub {
        my $self = shift;
        
        return $self->dbi->delete(table => $self->name, where => shift);
    };
    
    my $delete_all = sub {
        my $self = shift;
        
        return $self->dbi->delete_all(table => $self->name);
    };
    
    my $select = sub {
        my $self = shift;
        
        my $where  = {};
        my $column = ['*'];
        my $append = '';
        
        foreach my $arg (@_) {
            my $type = ref $arg;
            
            if ($type eq 'ARRAY') {
                $column = $arg;
            }
            elsif ($type eq 'HASH') {
                $where = $arg;
            }
            else {
                $append = $arg;
            }
        }
        
        return $self->dbi->select(
            table  => $self->name,
            where  => $where,
            column => $column,
            append => $append
        );
    };
    
    $self->helper(
        insert => $insert,
        insert_simple => $insert,
        update => $update,
        update_simple => $update,
        update_all => $update_all,
        update_all_simple => $update_all,
        delete => $delete,
        delete_simple => $delete,
        delete_all => $delete_all,
        delete_all_simple => $delete_all,
        select => $select,
        select_simple => $select
    );
    
    return $self;
}

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

DBIx::Custom::Model - Modele base class(experimental)

=head1 SYNOPSIS

use DBIx::Custom::Table;

my $table = DBIx::Custom::Table->new(name => 'books');

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<helper>

    $table->helper(insert => sub {
        # ...
    });

=head2 C<new>

    my $table = DBIx::Custom->new;
    
=head2 C<insert>

    $table->insert(\%param);

Insert.

=head2 C<insert_simple>

Same as C<insert()>.

=head2 C<update>

    $table->update(\%param, \%where);

Update.

=head2 C<update_simple>

Same as C<update()>.

=head2 C<update_all>

    $table->update_all(\%param);

Update all.

=head2 C<update_all_simple>

Same as C<update_all>.

=head2 C<delete>

    $table->delete(\%where);

=head2 C<delete_simple()>

Same as C<delete_all()>.

=head2 C<delete_all>

    $table->delete_all(\%where);

=head2 C<delete_all_simple()>

Same as C<delete_all()>.

=head2 C<select>

    $table->select(\%where);
    $table->select(\@column);
    $table->select($append);
    
    # And any combination
    $table->select(\%where, \@column, $append);

=head2 C<select_simple>

Same as C<select()>.

