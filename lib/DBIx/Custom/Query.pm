package DBIx::Custom::Query;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr([qw/sql columns default_filter filter sth/]);

1;

=head1 NAME

DBIx::Custom::Query - Query

=head1 SYNOPSIS
    
    my $query = DBIx::Custom::Query->new;
    
=head1 ATTRIBUTES

=head2 C<sql>

    $query = $query->sql($sql);
    $sql   = $query->sql;

SQL statement.

=head2 C<columns>

    $query   = $query->columns([@columns]);
    $columns = $query->columns;

Column names.

=head2 C<default_filter>

    $query          = $query->default_filter($filter);
    $default_filter = $query->default_filter;

Default filter for value binding.

=head2 C<filter>

    $query  = $query->filter({%filter});
    $filter = $query->filter;

Filters for value binding

=head2 C<sth>

    $query = $query->sth($sth);
    $sth   = $query->sth;

Statement handle.

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>.

=cut
