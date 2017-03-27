package DBIx::Custom::Result;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util qw/_array_to_hash _subname _deprecate/;

has [qw/dbi sth/];
has stash => sub { {} };

*all = \&fetch_hash_all;

sub fetch {
  my $self = shift;
  
  # Fetch
  my @row = $self->{sth}->fetchrow_array;
  return unless @row;

  if ($self->{_has_filter}) {
    # Info
    $self->_cache unless $self->{_cache};
    
    # Type rule
    if ($self->{type_rule}->{from1} && !$self->{type_rule_off} && !$self->{type_rule1_off}) {
      my $from = $self->{type_rule}->{from1};
      for my $type (keys %$from) {
        for my $column (@{$self->{_type_map}->{$type}}) {
          $row[$_] = $from->{$type}->($row[$_])
            for @{$self->{_pos}{$column} || []};
        }
      }
    }
    if ($self->{type_rule}->{from2} && !$self->{type_rule_off} && !$self->{type_rule2_off}) {
      my $from = $self->{type_rule}->{from2};
      for my $type (keys %$from) {
        for my $column (@{$self->{_type_map}->{$type}}) {
          $row[$_] = $from->{$type}->($row[$_])
            for @{$self->{_pos}{$column} || []};
        }
      }
    }
    
    # Filter
    if ($self->{filter}) {
       my @columns = keys %{$self->{filter}};
       
       for my $column (@columns) {
         my $filter = $self->{filter}{$column};
         next unless $filter;
         $row[$_] = $filter->($row[$_])
           for @{$self->{_pos}{$column} || []};
       }
    }
  }
  
  \@row;
}

sub fetch_hash {
  my $self = shift;
  
  # Fetch
  return unless my $row = $self->{sth}->fetchrow_hashref;
  
  if ($self->{_has_filter}) {
    
    # Info
    $self->_cache unless $self->{_cache};
    
    # Type rule
    if ($self->{type_rule}->{from1} &&
      !$self->{type_rule_off} && !$self->{type_rule1_off})
    {
      my $from = $self->{type_rule}->{from1};
      for my $type (keys %$from) {
        $from->{$type} and $row->{$_} = $from->{$type}->($row->{$_})
          for @{$self->{_type_map}->{$type}};
      }
    }
    if ($self->{type_rule}->{from2} &&
      !$self->{type_rule_off} && !$self->{type_rule2_off})
    {
      my $from = $self->{type_rule}->{from2};
      for my $type (keys %{$self->{type_rule}->{from2}}) {
        $from->{$type} and $row->{$_} = $from->{$type}->($row->{$_})
          for @{$self->{_type_map}->{$type}};
      }
    }        
    # Filter
    if ($self->{filter}) {
       my @columns = keys %{$self->{filter}};
       
       for my $column (@columns) {
         next unless exists $row->{$column};
         my $filter = $self->{filter}->{$column};
         $row->{$column} = $filter->($row->{$column}) if $filter;
       }
    }
  }
  
  $row;
}

sub fetch_all {
  my $self = shift;
  
  # Fetch all rows
  my $rows = [];
  while(my $row = $self->fetch) { push @$rows, $row}
  
  return $rows;
}

sub fetch_hash_all {
  my $self = shift;
  
  # Fetch all rows as hash
  my $rows = [];
  while(my $row = $self->fetch_hash) { push @$rows, $row }
  
  return $rows;
}

sub fetch_hash_one {
  my $self = shift;
  
  # Fetch hash
  my $row = $self->fetch_hash;
  return unless $row;
  
  # Finish statement handle
  $self->sth->finish;
  
  return $row;
}

sub fetch_hash_multi {
  my ($self, $count) = @_;
  
  # Fetch multiple rows
  croak 'Row count must be specified ' . _subname
    unless $count;
  
  return if $self->{_finished};

  my $rows = [];
  for (my $i = 0; $i < $count; $i++) {
    my $row = $self->fetch_hash;
    unless ($row) {
      $self->{_finished} = 1;
      last;
    }
    push @$rows, $row;
  }
  
  return unless @$rows;
  return $rows;
}

sub fetch_multi {
  my ($self, $count) = @_;
  
  # Row count not specified
  croak 'Row count must be specified ' . _subname
    unless $count;
  
  return if $self->{_finished};
  
  # Fetch multi rows
  my $rows = [];
  for (my $i = 0; $i < $count; $i++) {
    my $row = $self->fetch;
    unless ($row) {
      $self->{_finished} = 1;
      last;
    }
    push @$rows, $row;
  }
  
  return unless @$rows;
  return $rows;
}


sub fetch_one {
  my $self = shift;
  
  # Fetch
  my $row = $self->fetch;
  return unless $row;
  
  # Finish statement handle
  $self->sth->finish;
  
  return $row;
}

sub filter {
  my $self = shift;
  
  $self->{_has_filter} = 1;
  
  # Set
  if (@_) {
    
    # Convert filter name to subroutine
    my $filter = @_ == 1 ? $_[0] : [@_];
    $filter = _array_to_hash($filter);
    for my $column (keys %$filter) {
      my $fname = $filter->{$column};
      if  (exists $filter->{$column}
        && defined $fname
        && ref $fname ne 'CODE') 
      {
        croak qq{Filter "$fname" is not registered" } . _subname
          unless exists $self->dbi->filters->{$fname};
        $filter->{$column} = $self->dbi->filters->{$fname};
      }
    }
    
    # Merge
    $self->{filter} = {%{$self->filter}, %$filter};
    
    return $self;
  }
  
  return $self->{filter} ||= {};
}

sub flat {
  my $self = shift;
  
  my @flat;
  while (my $row = $self->fetch) {
    push @flat, @$row;
  }
  return @flat;
}

sub kv {
  my ($self, %opt) = @_;

  my $key_name = $self->{sth}{NAME}[0];
  my $kv = {};
  while (my $row = $self->fetch_hash) {
    my $key_value = delete $row->{$key_name};
    next unless defined $key_value;
    if ($opt{multi}) {
      _deprecate('0.28', "DBIx::Custom::Result::kv method's "
        . 'multi option is DEPRECATED. use kvs method instead');
      $kv->{$key_value} ||= [];
      push @{$kv->{$key_value}}, $row;
    }
    else { $kv->{$key_value} = $row }
  }
  
  return $kv;
}

sub kvs {
  my ($self, %opt) = @_;

  my $key_name = $self->{sth}{NAME}[0];
  my $kv = {};
  while (my $row = $self->fetch_hash) {
    my $key_value = delete $row->{$key_name};
    next unless defined $key_value;
    $kv->{$key_value} ||= [];
    push @{$kv->{$key_value}}, $row;
  }
  
  return $kv;
}

sub header { shift->sth->{NAME} }

*one = \&fetch_hash_one;

sub type_rule {
  my $self = shift;
  
  $self->{_has_filter} = 1;
  
  if (@_) {
    my $type_rule = ref $_[0] eq 'HASH' ? $_[0] : {@_};

    # From
    for my $i (1 .. 2) {
      $type_rule->{"from$i"} = _array_to_hash($type_rule->{"from$i"});
      for my $data_type (keys %{$type_rule->{"from$i"} || {}}) {
        croak qq{data type of from$i section must be lower case or number}
          if $data_type =~ /[A-Z]/;
        my $fname = $type_rule->{"from$i"}{$data_type};
        if (defined $fname && ref $fname ne 'CODE') {
          croak qq{Filter "$fname" is not registered" } . _subname
            unless exists $self->dbi->filters->{$fname};
          
          $type_rule->{"from$i"}{$data_type} = $self->dbi->filters->{$fname};
        }
      }
    }
    $self->{type_rule} = $type_rule;
    
    return $self;
  }
  
  return $self->{type_rule} || {};
}

sub type_rule_off {
  my $self = shift;
  $self->{type_rule_off} = 1;
  return $self;
}

sub type_rule_on {
  my $self = shift;
  $self->{type_rule_off} = 0;
  return $self;
}

sub type_rule1_off {
  my $self = shift;
  $self->{type_rule1_off} = 1;
  return $self;
}

sub type_rule1_on {
  my $self = shift;
  $self->{type_rule1_off} = 0;
  return $self;
}

sub type_rule2_off {
  my $self = shift;
  $self->{type_rule2_off} = 1;
  return $self;
}

sub type_rule2_on {
  my $self = shift;
  $self->{type_rule2_off} = 0;
  return $self;
}

sub value {
  my $self = shift;
  my $row = $self->fetch_one;
  my $value = $row ? $row->[0] : undef;
  return $value;
}

sub values {
  my $self = shift;
  
  my $values = [];
  my $rows = $self->fetch_all;
  push @$values, $_->[0] for @$rows;
  return $values;
}

sub _cache {
  my $self = shift;
  $self->{_type_map} = {};
  $self->{_pos} = {};
  $self->{_columns} = {};
  for (my $i = 0; $i < @{$self->{sth}->{NAME} || []}; $i++) {
    my $type = lc $self->{sth}{TYPE}[$i];
    my $name = $self->{sth}{NAME}[$i];
    $self->{_type_map}{$type} ||= [];
    push @{$self->{_type_map}{$type}}, $name;
    $self->{_pos}{$name} ||= [];
    push @{$self->{_pos}{$name}}, $i;
    $self->{_columns}{$name} = 1;
  }
  $self->{_cache} = 1;
}

1;

=head1 NAME

DBIx::Custom::Result - Result of select statement

=head1 SYNOPSIS

  # Result
  my $result = $dbi->select(table => 'book');

  # Fetch a row and put it into array reference
  while (my $row = $result->fetch) {
    my $author = $row->[0];
    my $title  = $row->[1];
  }
  
  # Fetch only a first row and put it into array reference
  my $row = $result->fetch_one;
  
  # Fetch all rows and put them into array of array reference
  my $rows = $result->fetch_all;

  # Fetch a row and put it into hash reference
  while (my $row = $result->fetch_hash) {
    my $title  = $row->{title};
    my $author = $row->{author};
  }
  
  # Fetch only a first row and put it into hash reference
  my $row = $result->fetch_hash_one;
  my $row = $result->one; # Alias for "fetch_hash_one"
  
  # Fetch all rows and put them into array of hash reference
  my $rows = $result->fetch_hash_all;
  my $rows = $result->all; # Alias for "fetch_hash_all"

=head1 ATTRIBUTES

=head2 dbi

  my $dbi = $result->dbi;
  $result = $result->dbi($dbi);

L<DBIx::Custom> object.

=head2 sth

  my $sth = $reuslt->sth
  $result = $result->sth($sth);

Statement handle of L<DBI>.

=head1 METHODS

L<DBIx::Custom::Result> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 all

  my $rows = $result->all;

Same as fetch_hash_all.

=head2 fetch

  my $row = $result->fetch;

Fetch a row and put it into array reference.

=head2 fetch_all

  my $rows = $result->fetch_all;

Fetch all rows and put them into array of array reference.

=head2 fetch_one

  my $row = $result->fetch_one;

Fetch only a first row and put it into array reference,
and finish statement handle.

=head2 fetch_hash

  my $row = $result->fetch_hash;

Fetch a row and put it into hash reference.

=head2 fetch_hash_all

  my $rows = $result->fetch_hash_all;

Fetch all rows and put them into array of hash reference.

=head2 fetch_hash_one
  
  my $row = $result->fetch_hash_one;

Fetch only a first row and put it into hash reference,
and finish statement handle.

=head2 fetch_hash_multi

  my $rows = $result->fetch_hash_multi(5);
  
Fetch multiple rows and put them into array of hash reference.

=head2 fetch_multi

  my $rows = $result->fetch_multi(5);
  
Fetch multiple rows and put them into array of array reference.

=head2 filter

  $result->filter(title  => sub { uc $_[0] }, author => 'to_upper');
  $result->filter([qw/title author/] => 'to_upper');

Set filter for column.
You can use subroutine or filter name as filter.
This filter is executed after C<type_rule> filter.

=head2 flat

  my @list = $result->flat;

All values is added to flatten list.
  
  my @list = $dbi->select(['id', 'title'])->flat;

C<flat> method return the following data.

  (1, 'Perl', 2, 'Ruby')

You can create key-value pair easily.

  my %titles = $dbi->select(['id', 'title'])->flat;

=head2 kv

  my $key_value = $result->kv;

Get key-value pairs.

  my $books = $dbi->select(['id', 'title', 'author'])->kv;

If C<all> method return the following data:

  [
    {id => 1, title => 'Perl', author => 'Ken'},
    {id => 2, title => 'Ruby', author => 'Taro'}
  ]

C<kv> method return the following data.

  {
    1 => {title => 'Perl', author => 'Ken'},
    2 => {title => 'Ruby', author => 'Taro'}
  }

First column value become key.

=head2 kvs

  my $key_values = $result->kvs;

Get key-values pairs.

  my $books = $dbi->select(['author', 'title', 'price'])->kvs;

If C<all> method return the following data:

  [
    {author => 'Ken', title => 'Perl', price => 1000},
    {author => 'Ken', title => 'Good', price => 2000},
    {author => 'Taro', title => 'Ruby', price => 3000}
    {author => 'Taro', title => 'Sky', price => 4000}
  ]

C<kvs> method return the following data.

  {
    Ken => [
      {title => 'Perl', price => 1000},
      {title => 'Good', price => 2000}
    ],
    Taro => [
      {title => 'Ruby', price => 3000},
      {title => 'Sky', price => 4000}
    ]
  }

=head2 header

  my $header = $result->header;

Get header column names.

=head2 one

  my $row = $result->one;

Alias for C<fetch_hash_one>.

=head2 stash

  my $stash = $result->stash;
  my $foo = $result->stash->{foo};
  $result->stash->{foo} = $foo;

Stash is hash reference to save some data.

=head2 type_rule
  
  # Merge type rule
  $result->type_rule(
    # DATE
    9 => sub { ... },
    # DATETIME or TIMESTAMP
    11 => sub { ... }
  );

  # Replace type rule(by reference)
  $result->type_rule([
    # DATE
    9 => sub { ... },
    # DATETIME or TIMESTAMP
    11 => sub { ... }
  ]);

This is same as L<DBIx::Custom>'s C<type_rule>'s <from>.

=head2 type_rule_off

  $result = $result->type_rule_off;

Turn C<from1> and C<from2> type rule off.
By default, type rule is on.

=head2 type_rule_on

  $result = $result->type_rule_on;

Turn C<from1> and C<from2> type rule on.
By default, type rule is on.

=head2 type_rule1_off

  $result = $result->type_rule1_off;

Turn C<from1> type rule off.
By default, type rule is on.

=head2 type_rule1_on

  $result = $result->type_rule1_on;

Turn C<from1> type rule on.
By default, type rule is on.

=head2 type_rule2_off

  $result = $result->type_rule2_off;

Turn C<from2> type rule off.
By default, type rule is on.

=head2 type_rule2_on

  $result = $result->type_rule2_on;

Turn C<from2> type rule on.
By default, type rule is on.

=head2 value

  my $value = $result->value;

Get first column's first value.

  my $count = $dbi->select('count(*)', table => 'book')->value;

This is almost same as the following one.

  my $count = $dbi->select('count(*)')->fetch_one->[0];

=head2 values

  my $values = $result->values;

Get first column's values.

  my $tables = $dbi->select('show tables')->values;

This is same as the following one.

  my $rows = $dbi->select('show tables')->fetch_all;
  my $tables = [map { $_->[0] } @$rows];

=cut
