package DBIx::Custom::Query;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr([qw/sql columns default_filter filter sth/]);

1;

=head1 NAME

DBIx::Custom::Query - query used by DBIx::Custom

=head1 SYNOPSIS
    
    # New
    my $query = DBIx::Custom::Query->new;
    
=head1 ATTRIBUTES

=head2 sql

SQL statement.

    $query = $query->sql($sql);
    $sql   = $query->sql;

=head2 columns

Column names.

    $query   = $query->columns([@columns]);
    $columns = $query->columns;

=head2 default_filter

Default filter.

    $query          = $query->default_filter($filter);
    $default_filter = $query->default_filter;

=head2 filter

Filter.

    $query  = $query->filter({%filter});
    $filter = $query->filter;

=head2 sth

Statement handle.

    $query = $query->sth($sth);
    $sth   = $query->sth;

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>.

=cut
