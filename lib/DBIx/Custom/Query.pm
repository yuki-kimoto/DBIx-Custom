package DBIx::Custom::Query;

use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util '_subname';

has [qw/sth filters/],
    sql => '',
    tables => sub { [] },
    columns => sub { [] };

sub filter {
    my $self = shift;
    
    if (@_) {
        my $filter = {};
        
        if (ref $_[0] eq 'HASH') {
            $filter = $_[0];
        }
        else {
            my $ef = @_ > 1 ? [@_] : $_[0];
            
            for (my $i = 0; $i < @$ef; $i += 2) {
                my $column = $ef->[$i];
                my $f = $ef->[$i + 1];
                
                if (ref $column eq 'ARRAY') {
                    foreach my $c (@$column) {
                        $filter->{$c} = $f;
                    }
                }
                else {
                    $filter->{$column} = $f;
                }
            }
        }
        
        foreach my $column (keys %$filter) {
            my $fname = $filter->{$column};

            if  (exists $filter->{$column}
              && defined $fname
              && ref $fname ne 'CODE') 
            {
              croak qq{Filter "$fname" is not registered" } . _subname
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

    $query     = $query->filter([qw/author title/] => 'to_something');

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
