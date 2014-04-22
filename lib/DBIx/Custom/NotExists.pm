package DBIx::Custom::NotExists;

use strict;
use warnings;

my $not_exists = bless {}, 'DBIx::Custom::NotExists';

sub singleton { $not_exists }

=head1 NAME

DBIx::Custom::NotExists - Not exists object

=head1 SYNOPSYS

  $not_exists = DBIx::Custom::NotExists->singleton;

=head1 METHODS

=head2 singleton

  $not_exists = DBIx::Custom::NotExists->singleton;

L<DBIx::Custom::NotExists> singleton object.

=cut
