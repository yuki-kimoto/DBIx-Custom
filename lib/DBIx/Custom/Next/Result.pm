package DBIx::Custom::Next::Result;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Next::Util qw/_array_to_hash _subname/;

has [qw/dbi sth/],
    stash => sub { {} };

*all = \&fetch_hash_all;

sub filter {
    my $self = shift;
    
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

sub fetch {
    my $self = shift;
    
    # Info
    $self->_cache unless $self->{_cache};
    
    # Fetch
    my @row = $self->{sth}->fetchrow_array;
    return unless @row;
    
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
         for my $column (keys %{$self->{filter}}) {
             my $filter = $self->{filter}->{$column};
             next unless $filter;
             $row[$_] = $filter->($row[$_])
               for @{$self->{_pos}{$column} || []};
         }
    }
    return \@row;
}

sub fetch_hash {
    my $self = shift;
    
    # Info
    $self->_cache unless $self->{_cache};
    
    # Fetch
    return unless my $row = $self->{sth}->fetchrow_hashref;
    
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
       exists $row->{$_} && $self->{filter}->{$_}
           and $row->{$_} = $self->{filter}->{$_}->($row->{$_})
         for keys %{$self->{filter}};
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

sub fetch_first {
    my $self = shift;
    
    # Fetch
    my $row = $self->fetch;
    return unless $row;
    
    # Finish statement handle
    $self->sth->finish;
    
    return $row;
}

sub fetch_hash_all {
    my $self = shift;
    
    # Fetch all rows as hash
    my $rows = [];
    while(my $row = $self->fetch_hash) { push @$rows, $row }
    
    return $rows;
}

sub fetch_hash_first {
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
    
    # Row count not specifed
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

sub header { shift->sth->{NAME} }

*one = \&fetch_hash_first;

sub type_rule {
    my $self = shift;
    
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

sub _cache {
    my $self = shift;
    $self->{_type_map} = {};
    $self->{_pos} = {};
    $self->{_columns} = {};
    for (my $i = 0; $i < @{$self->{sth}->{NAME}}; $i++) {
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

DBIx::Custom::Next::Result - Result of select statement

=head1 SYNOPSIS

    # Result
    my $result = $dbi->select(table => 'book');

    # Fetch a row and put it into array reference
    while (my $row = $result->fetch) {
        my $author = $row->[0];
        my $title  = $row->[1];
    }
    
    # Fetch only a first row and put it into array reference
    my $row = $result->fetch_first;
    
    # Fetch all rows and put them into array of array reference
    my $rows = $result->fetch_all;

    # Fetch a row and put it into hash reference
    while (my $row = $result->fetch_hash) {
        my $title  = $row->{title};
        my $author = $row->{author};
    }
    
    # Fetch only a first row and put it into hash reference
    my $row = $result->fetch_hash_first;
    my $row = $result->one; # Same as fetch_hash_first
    
    # Fetch all rows and put them into array of hash reference
    my $rows = $result->fetch_hash_all;
    my $rows = $result->all; # Same as fetch_hash_all

=head1 ATTRIBUTES

=head2 C<dbi>

    my $dbi = $result->dbi;
    $result = $result->dbi($dbi);

L<DBIx::Custom::Next> object.

=head2 C<sth>

    my $sth = $reuslt->sth
    $result = $result->sth($sth);

Statement handle of L<DBI>.

=head1 METHODS

L<DBIx::Custom::Next::Result> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<all>

    my $rows = $result->all;

Same as C<fetch_hash_all>.

=head2 C<fetch>

    my $row = $result->fetch;

Fetch a row and put it into array reference.

=head2 C<fetch_all>

    my $rows = $result->fetch_all;

Fetch all rows and put them into array of array reference.

=head2 C<fetch_first>

    my $row = $result->fetch_first;

Fetch only a first row and put it into array reference,
and finish statment handle.

=head2 C<fetch_hash>

    my $row = $result->fetch_hash;

Fetch a row and put it into hash reference.

=head2 C<fetch_hash_all>

    my $rows = $result->fetch_hash_all;

Fetch all rows and put them into array of hash reference.

=head2 C<fetch_hash_first>
    
    my $row = $result->fetch_hash_first;

Fetch only a first row and put it into hash reference,
and finish statment handle.

=head2 C<fetch_hash_multi>

    my $rows = $result->fetch_hash_multi(5);
    
Fetch multiple rows and put them into array of hash reference.

=head2 C<fetch_multi>

    my $rows = $result->fetch_multi(5);
    
Fetch multiple rows and put them into array of array reference.

=head2 C<filter>

    $result->filter(title  => sub { uc $_[0] }, author => 'to_upper');
    $result->filter([qw/title author/] => 'to_upper');

Set filter for column.
You can use subroutine or filter name as filter.
This filter is executed after C<type_rule> filter.

=head2 C<header>

    my $header = $result->header;

Get header column names.

=head2 C<one>

    my $row = $result->one;

Same as C<fetch_hash_first>.

=head2 C<stash>

    my $stash = $result->stash;
    my $foo = $result->stash->{foo};
    $result->stash->{foo} = $foo;

Stash is hash reference for data.

=head2 C<type_rule>
    
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

This is same as L<DBIx::Custom::Next>'s C<type_rule>'s <from>.

=head2 C<type_rule_off>

    $result = $result->type_rule_off;

Turn C<from1> and C<from2> type rule off.
By default, type rule is on.

=head2 C<type_rule_on>

    $result = $result->type_rule_on;

Turn C<from1> and C<from2> type rule on.
By default, type rule is on.

=head2 C<type_rule1_off>

    $result = $result->type_rule1_off;

Turn C<from1> type rule off.
By default, type rule is on.

=head2 C<type_rule1_on>

    $result = $result->type_rule1_on;

Turn C<from1> type rule on.
By default, type rule is on.

=head2 C<type_rule2_off>

    $result = $result->type_rule2_off;

Turn C<from2> type rule off.
By default, type rule is on.

=head2 C<type_rule2_on>

    $result = $result->type_rule2_on;

Turn C<from2> type rule on.
By default, type rule is on.

=cut
