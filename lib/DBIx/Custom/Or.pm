package DBIx::Custom::Or;

use strict;
use warnings;

use base 'Object::Simple';

__PACKAGE__->attr(values => sub { [] });

1;

=head1 NAME

DBIx::Custom::Or - or condition

=head1 SYNOPSYS

    my $or = DBIx::Custom::Or->new;

=head1 ATTRIBUTES

=head2 C<values>

    my $values = $or->values;
    $or        = $or->values([1, 2]);

