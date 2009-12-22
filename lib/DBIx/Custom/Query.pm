package DBIx::Custom::Query;
use base 'Object::Simple::Base';

use strict;
use warnings;

my $p = __PACKAGE__;

$p->attr([qw/sql key_infos bind_filter fetch_filter sth/])
  ->attr(_no_bind_filters_map => sub { {} })
  ->attr(no_fetch_filters => (type => 'array', default => sub { [] }));

$p->attr(no_bind_filters => (type => 'array', trigger => sub {
    my $self = shift;
    my $no_bind_filters = $self->no_bind_filters || [];
    my %no_bind_filters_map = map {$_ => 1} @{$no_bind_filters};
    $self->_no_bind_filters_map(\%no_bind_filters_map);
}));

sub new {
    my $self = shift->SUPER::new(@_);
    
    Object::Simple::Util->init_attrs($self, 'no_bind_filters');
    
    return $self;
}

=head1 NAME

DBIx::Custom::Query - DBIx::Custom query

=head1 SYNOPSIS
    
    # New
    my $query = DBIx::Custom::Query->new;
    
    # Create by using create_query
    my $query = DBIx::Custom->create_query($template);
    
    # Set attributes
    $query->bind_filter($dbi->filters->{default_bind_filter});
    $query->no_bind_filters('title', 'author');
    
    $query->fetch_filter($dbi->filters->{default_fetch_filter});
    $query->no_fetch_filters('title', 'author');

=head1 Accessors

=head2 sth

Set and get statement handle

    $query = $query->sth($sth);
    $sth   = $query->sth;

=head2 sql

Set and get SQL

    $query = $query->sql($sql);
    $sql   = $query->sql;

=head2 bind_filter

Set and get bind filter

    $query       = $query->bind_filter($bind_filter);
    $bind_filter = $query->bind_filter;

=head2 no_bind_filters

Set and get keys of no filtering

    $query           = $query->no_bind_filters($no_filters);
    $no_bind_filters = $query->no_bind_filters;

=head2 fetch_filter

Set and get fetch filter

    $query        = $query->fetch_filter($fetch_filter);
    $fetch_filter = $query->fetch_filter;

=head2 no_fetch_filters

Set and get keys of no filtering

    $query            = $query->no_fetch_filters($no_filters);
    $no_fetch_filters = $query->no_fetch_filters;

=head2 key_infos

Set and get key informations

    $query     = $query->key_infos($key_infos);
    $key_infos = $query->key_infos;

=head1 Methods

=head2 new

    my $query = DBIx::Custom::Query->new;
    
=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.
