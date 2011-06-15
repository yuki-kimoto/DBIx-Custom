package DBIx::Custom::Result;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util qw/_array_to_hash _subname/;

has [qw/filters filter_off sth type_rule_off/];
has stash => sub { {} };

*all = \&fetch_hash_all;

sub filter {
    my $self = shift;
    
    # Set
    if (@_) {
        
        # Convert filter name to subroutine
        my $filter = @_ == 1 ? $_[0] : [@_];
        $filter = _array_to_hash($filter);
        foreach my $column (keys %$filter) {
            my $fname = $filter->{$column};
            if  (exists $filter->{$column}
              && defined $fname
              && ref $fname ne 'CODE') 
            {
              croak qq{Filter "$fname" is not registered" } . _subname
                unless exists $self->filters->{$fname};
              $filter->{$column} = $self->filters->{$fname};
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
    
    # Fetch
    my @row = $self->{sth}->fetchrow_array;
    return unless @row;
    
    # Filtering
    my $columns = $self->{sth}->{NAME};
    my $types = $self->{sth}->{TYPE};
    my $type_rule = $self->type_rule || {};
    my $filter = $self->filter;
    my $end_filter = $self->end_filter;
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Column
        my $column = $columns->[$i];
        
        # Type rule
        my $type_filter = $type_rule->{lc($types->[$i])};
        $row[$i] = $type_filter->($row[$i])
          if $type_filter && !$self->{type_rule_off};
        
        # Filter
        my $filter  = $filter->{$column} || $self->{default_filter};
        $row[$i] = $filter->($row[$i])
          if $filter && !$self->{filter_off};
        $row[$i] = $end_filter->{$column}->($row[$i])
          if $end_filter->{$column} && !$self->{filter_off};
    }

    return \@row;
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

sub fetch_hash {
    my $self = shift;
    
    # Fetch
    my $row = $self->{sth}->fetchrow_arrayref;
    return unless $row;

    # Filter
    my $hash_row = {};
    my $filter  = $self->filter;
    my $end_filter = $self->end_filter || {};
    my $columns = $self->{sth}->{NAME};
    my $types = $self->{sth}->{TYPE};
    my $type_rule = $self->type_rule || {};
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Column
        my $column = $columns->[$i];
        
        # Type rule
        my $type_filter = $type_rule->{lc($types->[$i])};
        if (!$self->{type_rule_off} && $type_filter) {
            $hash_row->{$column} = $type_filter->($row->[$i]);
        }
        else { $hash_row->{$column} = $row->[$i] }
        
        # Filter
        my $f = $filter->{$column} || $self->{default_filter};
        $hash_row->{$column} = $f->($hash_row->{$column})
          if $f && !$self->{filter_off};
        $hash_row->{$column} = $end_filter->{$column}->($hash_row->{$column})
          if $end_filter->{$column} && !$self->{filter_off};
    }
    
    return $hash_row;
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
    my $rows = [];
    for (my $i = 0; $i < $count; $i++) {
        my $row = $self->fetch_hash;
        last unless $row;
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
    
    # Fetch multi rows
    my $rows = [];
    for (my $i = 0; $i < $count; $i++) {
        my $row = $self->fetch;
        last unless $row;
        push @$rows, $row;
    }
    
    return unless @$rows;
    return $rows;
}

*one = \&fetch_hash_first;

sub type_rule {
    my $self = shift;
    
    # Merge type rule
    if (@_) {
        my $type_rule = @_ == 1 ? $_[0] : [@_];
        $type_rule = _array_to_hash($type_rule) || {};
        foreach my $data_type (keys %{$type_rule || {}}) {
            croak qq{data type of into section must be lower case or number}
              if $data_type =~ /[A-Z]/;
            my $fname = $type_rule->{$data_type};
            if (defined $fname && ref $fname ne 'CODE') {
                croak qq{Filter "$fname" is not registered" } . _subname
                  unless exists $self->filters->{$fname};
                
                $type_rule->{$data_type} = $self->filters->{$fname};
            }
        }
        
        # Replace
        if (@_ == 1) { $self->{type_rule} = $type_rule }
        # Merge
        else { $self->{type_rule} = {%{$self->type_rule}, %$type_rule} }
    }
    
    return $self->{type_rule} ||= {};
}

# DEPRECATED!
sub end_filter {
    my $self = shift;
    if (@_) {
        my $end_filter = {};
        if (ref $_[0] eq 'HASH') { $end_filter = $_[0] }
        else { 
            $end_filter = _array_to_hash(
                @_ > 1 ? [@_] : $_[0]
            );
        }
        foreach my $column (keys %$end_filter) {
            my $fname = $end_filter->{$column};
            if  (exists $end_filter->{$column}
              && defined $fname
              && ref $fname ne 'CODE') 
            {
              croak qq{Filter "$fname" is not registered" } . _subname
                unless exists $self->filters->{$fname};
              $end_filter->{$column} = $self->filters->{$fname};
            }
        }
        $self->{end_filter} = {%{$self->end_filter}, %$end_filter};
        return $self;
    }
    return $self->{end_filter} ||= {};
}

# DEPRECATED!
sub remove_end_filter {
    my $self = shift;
    warn "remove_end_filter is DEPRECATED! use filter_off attribute instead";
    $self->{end_filter} = {};
    return $self;
}

# DEPRECATED!
sub remove_filter {
    my $self = shift;
    warn "remove_filter is DEPRECATED! use filter_off attribute instead";
    $self->{filter} = {};
    return $self;
}

# DEPRECATED!
sub default_filter {
    my $self = shift;
    warn "default_filter is DEPRECATED!";
    if (@_) {
        my $fname = $_[0];
        if (@_ && !$fname) {
            $self->{default_filter} = undef;
        }
        else {
            croak qq{Filter "$fname" is not registered}
              unless exists $self->filters->{$fname};
            $self->{default_filter} = $self->filters->{$fname};
        }
        return $self;
    }
    return $self->{default_filter};
}

# DEPRECATED!
has 'filter_check'; 

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

=head2 C<filter_off> EXPERIMENTAL

    my $filter_off = $resutl->filter_off;
    $result = $result->filter_off(1);

Filtering by C<filter> method is turned off.

=head2 C<filters>

    my $filters = $result->filters;
    $result = $result->filters(\%filters);

Filters.

=head2 C<sth>

    my $sth = $reuslt->sth
    $result = $result->sth($sth);

Statement handle of L<DBI>.

=head2 C<type_rule_off> EXPERIMENTAL

    my $type_rule_off = $result->type_rule_off;
    $result = $result->type_rule_off(1);

Filtering by C<type_rule> is turned off.

=head1 METHODS

L<DBIx::Custom::Result> inherits all methods from L<Object::Simple>
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

=head2 C<one>

    my $row = $result->one;

Same as C<fetch_hash_first>.

=head2 C<stash>

    my $stash = $result->stash;
    my $foo = $result->stash->{foo};
    $result->stash->{foo} = $foo;

Stash is hash reference for data.

=head2 C<type_rule> EXPERIMENTAL
    
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

=cut
