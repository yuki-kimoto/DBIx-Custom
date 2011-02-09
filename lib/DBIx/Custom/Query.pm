package DBIx::Custom::Query;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr([qw/columns sql sth filters/]);

sub filter {
    my $self = shift;
    
    if (@_) {
        my $filter = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        foreach my $column (keys %$filter) {
            my $fname = $filter->{$column};

            if  (exists $filter->{$column}
              && defined $fname
              && ref $fname ne 'CODE') 
            {
              croak qq{Filter "$fname" is not registered"}
                unless exists $self->filters->{$fname};
              
              $filter->{$column} = $self->filters->{$fname};
            }
        }
        
        $self->{filter} = {%{$self->filter}, %$filter};
        
        return $self;
    }
    
    return $self->{filter} ||= {};
}

# DEPRECATED!
__PACKAGE__->attr('default_filter');

1;

=head1 NAME

DBIx::Custom::Query - Query

=head1 SYNOPSIS
    
    my $query = DBIx::Custom::Query->new;
    
=head1 ATTRIBUTES

=head2 C<columns>

    my $columns = $query->columns;
    $query      = $query->columns(['auhtor', 'title']);

Column names.

=head2 C<filter>

    my $filter = $query->filter;
    $query     = $query->filter(author => 'to_something',
                                 title  => 'to_something');

Filters when parameter binding is executed.

=head2 C<sql>

    my $sql = $query->sql;
    $query  = $query->sql('select * from books where author = ?;');

SQL statement.

=head2 C<sth>

    my $sth = $query->sth;
    $query  = $query->sth($sth);

Statement handle of L<DBI>

=head1 METHODS

L<DBIx::Custom::Query> inherits all methods from L<Object::Simple>.

=cut
