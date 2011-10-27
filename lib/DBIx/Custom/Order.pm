package DBIx::Custom::Order;
use Object::Simple -base;
use overload
  'bool'   => sub {1},
  '""'     => sub { shift->to_string },
  fallback => 1;

has 'dbi',
    orders => sub { [] };

sub prepend {
    my $self = shift;
    
    for my $order (reverse @_) {
        if (ref $order eq 'ARRAY') {
            my $column = shift @$order;
            $column = $self->dbi->q($column) if defined $column;
            my $derection = shift @$order;
            $order = $column;
            $order .= " $derection" if $derection;
        }
        unshift @{$self->orders}, $order;
    }
    
    return $self;
}

sub to_string {
    my $self = shift;
    
    my $exists = {};
    my @orders;
    for my $order (@{$self->orders}) {
        next unless defined $order;
        $order =~ s/^\s+//;
        $order =~ s/\s+$//;
        my ($column, $direction) = split /\s+/, $order;
        push @orders, $order unless $exists->{$column};
        $exists->{$column} = 1;
    }
    
    return '' unless @orders;
    return 'order by ' . join(', ', @orders);
}

1;

=head1 NAME

DBIx::Custom::Order - Order by

=head1 SYNOPSIS

    # Result
    my $order = DBIx::Custom::Order->new;
    $order->prepend('title', 'author desc');
    my $order_by = "$order";
    
=head1 ATTRIBUTES

=head2 C<dbi>

    my $dbi = $order->dbi;
    $order = $order->dbi($dbi);

L<DBIx::Custom> object.

=head2 C<orders>

    my $orders = $result->orders;
    $result = $result->orders(\%orders);

Parts of order by clause

=head1 METHODS

L<DBIx::Custom::Result> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<prepend>

    $order->prepend('title', 'author desc');

Prepend order parts to C<orders>.

You can pass array reference, which contain column name and direction.
Column name is quoted properly
    
    # Column name and direction
    $order->prepend(['book-title']);
    $order->prepend([qw/book-title desc/]);

This is expanded to the following way.

    "book-title"
    "book-title" desc

=head2 C<to_string>

    my $order_by = $order->to_string;

Create order by clause. If column name is duplicated, First one is used.
C<to_string> override stringification. so you can write the follwoing way.

    my $order_by = "$order";

=cut

