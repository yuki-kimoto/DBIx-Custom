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

    my $sql = $query->sql;
    $query  = $query->sql($sql);

SQL statement.

=head2 C<columns>

    my $columns = $query->columns;
    $query      = $query->columns(\@columns);

Column names.

=head2 C<default_filter>

    my $default_filter = $query->default_filter;
    $query             = $query->default_filter($filter);

Default filter for value binding.

=head2 C<filter>

    my $filter = $query->filter;
    $query     = $query->filter(\%filter);

Filters for value binding

=head2 C<sth>

    my $sth = $query->sth;
    $query  = $query->sth($sth);

Statement handle.

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>.

=cut
