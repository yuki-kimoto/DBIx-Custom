package DBIx::Custom::Query;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr([qw/sql columns default_filter filter sth/]);

1;

=head1 NAME

DBIx::Custom::Query - DBIx::Custom query

=head1 SYNOPSIS
    
    # New
    my $query = DBIx::Custom::Query->new;
    
    # Create by using create_query
    my $query = DBIx::Custom->create_query($template);
    
=head1 ATTRIBUTES

=head2 sth

Statement handle

    $query = $query->sth($sth);
    $sth   = $query->sth;

=head2 sql

SQL

    $query = $query->sql($sql);
    $sql   = $query->sql;

=head2 default_filter

Filter excuted when value is bind

    $query          = $query->default_filter($default_filter);
    $default_filter = $query->default_filter;

=head2 filter

Filter excuted when value is bind

    $query  = $query->filter($filter);
    $filter = $query->filter;

=head2 columns

Key informations

    $query   = $query->columns($columns);
    $columns = $query->columns;

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 new

    my $query = DBIx::Custom::Query->new;

=cut
