package DBIx::Custom::Next::NotExists;

use strict;
use warnings;

my $not_exists = bless {}, 'DBIx::Custom::Next::NotExists';

sub singleton { $not_exists }

=head1 NAME

DBIx::Custom::Next::NotExists

=head1 SYNOPSYS

    $not_exists = DBIx::Custom::Next::NotExists->singleton;

=head1 METHODS

=head2 C<singleton>

    $not_exists = DBIx::Custom::Next::NotExists->singleton;

L<DBIx::Custom::Next::NotExists> singleton object.

=cut
