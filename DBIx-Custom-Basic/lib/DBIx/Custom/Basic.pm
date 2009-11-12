package DBIx::Custom::Basic;
use base 'DBIx::Custom';
use Encode qw/decode encode/;

use warnings;
use strict;

our $VERSION = '0.0101';

my $class = __PACKAGE__;

$class->add_filter(
    default_bind_filter  => sub { encode('UTF-8', $_[1]) },
    default_fetch_filter => sub { decode('UTF-8', $_[1]) }
);

$class->bind_filter($class->filters->{default_bind_filter});
$class->fetch_filter($class->filters->{default_fetch_filter});

$class->add_format(
    'SQL99_date'        => '%Y-%m-%d',
    'SQL99_datetime'    => '%Y-%m-%d %H:%M:%S',
    'SQL99_time'        => '%H:%M:%S',
    'ISO-8601_date'     => '%Y-%m-%d',
    'ISO-8601_datetime' => '%Y-%m-%dT%H:%M:%S',
    'ISO-8601_time'     => '%H:%M:%S',
);

1;

=head1 NAME

DBIx::Custom::Basic - DBIx::Custom basic class

=head1 VERSION

Version 0.0101

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
