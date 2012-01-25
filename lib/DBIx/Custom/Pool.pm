package DBIx::Custom::Pool;
use Object::Simple -base;
use Carp 'croak';
use Digest::MD5 'md5_hex';

has count => 5;

sub prepare {
  my ($self, $cb) = @_;
  
  my $count = $self->count;
  for (my $i = 0; $i < $count; $i++) {
    my $dbi = $cb->();
    
    my $id = $self->_id;
    
    $self->{_pool}{$id} = $dbi;
  }
  return $self;
}

sub get {
  my $self = shift;
  
  my @ids = keys %{$self->{_pool}};
  croak "Pool is empty" unless @ids;
  my $id = $ids[0];
  my $dbi = delete $self->{_pool}{$id};
  $self->{_borrow}{$id} = 1;
  $dbi->{_pool_id} = $id;
  return $dbi;
}

sub back {
  my ($self, $dbi) = @_;
  my $id = $dbi->{_pool_id};
  return unless ref $dbi && defined $id;
  croak "This DBIx::Custom object is already returned back"
    if $self->{_pool}{$id};
  delete $self->{_borrow}{$id};
  $self->{_pool}{$id} = $dbi;
  
  return $self;
}

sub _id {
  my $self = shift;
  my $id;
  do { $id = md5_hex('c' . time . rand 999) }
    while $self->{_pool}->{$id} || $self->{_borrow}->{$id};
  return $id;
}

1;

=head1 NAME

DBIx::Custom::Pool

=head1 DESCRIPTION

DBI Pool. this module is very experimental.

