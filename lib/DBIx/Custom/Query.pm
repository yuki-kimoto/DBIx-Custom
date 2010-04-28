package DBIx::Custom::Query;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr([qw/sql key_infos query_filter fetch_filter sth/]);

1;

=head1 NAME

DBIx::Custom::Query - DBIx::Custom query

=head1 SYNOPSIS
    
    # New
    my $query = DBIx::Custom::Query->new;
    
    # Create by using create_query
    my $query = DBIx::Custom->create_query($template);
    
    # Attributes
    $query->query_filter($dbi->filters->{default_query_filter});
    $query->fetch_filter($dbi->filters->{default_fetch_filter});

=head1 ATTRIBUTES

=head2 sth

Statement handle

    $query = $query->sth($sth);
    $sth   = $query->sth;

=head2 sql

SQL

    $query = $query->sql($sql);
    $sql   = $query->sql;

=head2 query_filter

Filter excuted when value is bind

    $query       = $query->query_filter($query_filter);
    $query_filter = $query->query_filter;

=head2 fetch_filter

Filter excuted when data is fetched

    $query        = $query->fetch_filter($fetch_filter);
    $fetch_filter = $query->fetch_filter;

=head2 key_infos

Key informations

    $query     = $query->key_infos($key_infos);
    $key_infos = $query->key_infos;

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 new

    my $query = DBIx::Custom::Query->new;

=cut
