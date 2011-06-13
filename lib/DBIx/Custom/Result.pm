package DBIx::Custom::Result;

use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util qw/_array_to_hash _subname/;

has [qw/filters filter_off sth type_rule type_rule_off/],
    stash => sub { {} };

*all = \&fetch_hash_all;

sub filter {
    my $self = shift;
    
    if (@_) {
        my $filter = {};
        
        if (ref $_[0] eq 'HASH') {
            $filter = $_[0];
        }
        else {
            $filter = _array_to_hash(
                @_ > 1 ? [@_] : $_[0]
            );
        }
                
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
        
        $self->{filter} = {%{$self->filter}, %$filter};
        
        return $self;
    }
    
    return $self->{filter} ||= {};
}

sub end_filter {
    my $self = shift;
    
    if (@_) {
        my $end_filter = {};
        
        if (ref $_[0] eq 'HASH') {
            $end_filter = $_[0];
        }
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

sub fetch {
    my $self = shift;
    
    # Filter
    my $filter = $self->filter;
    
    # End filter
    my $end_filter = $self->end_filter;
    
    # Fetch
    my @row = $self->{sth}->fetchrow_array;
    
    # No row
    return unless @row;
    
    # Filtering
    my $columns = $self->{sth}->{NAME};
    my $types = $self->{sth}->{TYPE};
    my $type_rule = $self->type_rule || {};
    
    for (my $i = 0; $i < @$columns; $i++) {
        
        if (!$self->type_rule_off && $type_rule->{from} &&
            (my $rule = $type_rule->{from}->{$types->[$i]}))
        {
            $row[$i] = $rule->($row[$i]);
        }
        
        # Filter name
        my $column = $columns->[$i];
        my $f  = exists $filter->{$column}
               ? $filter->{$column}
               : $self->_default_filter;
        my $ef = $end_filter->{$column};
        
        # Filtering
        $row[$i] = $f->($row[$i]) if $f && !$self->filter_off;
        $row[$i] = $ef->($row[$i]) if $ef && !$self->filter_off;
    }

    return \@row;
}

sub fetch_all {
    my $self = shift;
    
    # Fetch all rows
    my $rows = [];
    while(my $row = $self->fetch) {
        push @$rows, $row;
    }
    return $rows;
}

sub fetch_first {
    my $self = shift;
    
    # Fetch
    my $row = $self->fetch;
    
    # No row
    return unless $row;
    
    # Finish statement handle
    $self->sth->finish;
    
    return $row;
}

sub fetch_hash {
    my $self = shift;
    
    # Filter
    my $filter  = $self->filter;
    
    # End filter
    my $end_filter = $self->end_filter;
    
    # Fetch
    my $row = $self->{sth}->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;

    # Filter
    my $row_hash = {};
    my $columns = $self->{sth}->{NAME};
    my $types = $self->{sth}->{TYPE};
    my $type_rule = $self->type_rule || {};
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Type rule
        if (!$self->type_rule_off && $type_rule->{from} &&
            (my $rule = $type_rule->{from}->{$types->[$i]}))
        {
            $row->[$i] = $rule->($row->[$i]);
        }
        
        # Filter name
        my $column = $columns->[$i];
        my $f  = exists $filter->{$column}
               ? $filter->{$column}
               : $self->_default_filter;
        my $ef = $end_filter->{$column};
        
        # Filtering
        $row_hash->{$column} = $f && !$self->filter_off ? $f->($row->[$i])
                                                        : $row->[$i];
        $row_hash->{$column} = $ef->($row_hash->{$column})
          if $ef && !$self->filter_off;
    }
    
    return $row_hash;
}

sub fetch_hash_all {
    my $self = shift;
    
    # Fetch all rows as hash
    my $rows = [];
    while(my $row = $self->fetch_hash) {
        push @$rows, $row;
    }
    
    return $rows;
}

sub fetch_hash_first {
    my $self = shift;
    
    # Fetch hash
    my $row = $self->fetch_hash;
    
    # No row
    return unless $row;
    
    # Finish statement handle
    $self->sth->finish;
    
    return $row;
}

sub fetch_hash_multi {
    my ($self, $count) = @_;
    
    # Row count not specified
    croak 'Row count must be specified ' . _subname
      unless $count;
    
    # Fetch multi rows
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
    return $self->_default_filter(@_)
}

# DEPRECATED!
sub _default_filter {
    my $self = shift;

    
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

Get the result of select statement.

    # Result
    my $result = $dbi->select(table => 'books');

Fetch row into array.
    
    # Fetch a row into array
    while (my $row = $result->fetch) {
        my $author = $row->[0];
        my $title  = $row->[1];
        
    }
    
    # Fetch only a first row into array
    my $row = $result->fetch_first;
    
    # Fetch multiple rows into array of array
    while (my $rows = $result->fetch_multi(5)) {
        my $first_author  = $rows->[0][0];
        my $first_title   = $rows->[0][1];
        my $second_author = $rows->[1][0];
        my $second_value  = $rows->[1][1];
    
    }
    
    # Fetch all rows into array of array
    my $rows = $result->fetch_all;

Fetch row into hash.

    # Fetch a row into hash
    while (my $row = $result->fetch_hash) {
        my $title  = $row->{title};
        my $author = $row->{author};
        
    }
    
    # Fetch only a first row into hash
    my $row = $result->fetch_hash_first;
    
    # Fetch multiple rows into array of hash
    while (my $rows = $result->fetch_hash_multi(5)) {
        my $first_title   = $rows->[0]{title};
        my $first_author  = $rows->[0]{author};
        my $second_title  = $rows->[1]{title};
        my $second_author = $rows->[1]{author};
    }
    
    # Fetch all rows into array of hash
    my $rows = $result->fetch_hash_all;

=head1 ATTRIBUTES

Filters when a row is fetched.
This overwrites C<default_filter>.

=head2 C<filter_off> EXPERIMENTAL

    my $filter_off = $resutl->filter_off;
    $result = $result->filter_off(1);

Turn filter off.

=head2 C<filters>

    my $filters = $result->filters;
    $result     = $result->filters(\%filters);

Resistered filters.

=head2 C<sth>

    my $sth = $reuslt->sth
    $result = $result->sth($sth);

Statement handle of L<DBI>.

=head2 C<type_rule_off> EXPERIMENTAL

    my $type_rule_off = $result->type_rule_off;
    $result = $result->type_rule_off(1);

Turn type rule off.

=head1 METHODS

L<DBIx::Custom::Result> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<all>

    my $rows = $result->all;

This is alias for C<fetch_hash_all>.

=head2 C<end_filter>

    $result = $result->end_filter(title  => 'to_something',
                                     author => 'to_something');

    $result = $result->end_filter([qw/title author/] => 'to_something');

End filters.
These each filters is executed after the filters applied by C<apply_filter> of
L<DBIx::Custom> or C<filter> method.

=head2 C<fetch>

    my $row = $result->fetch;

Fetch a row into array.

=head2 C<fetch_all>

    my $rows = $result->fetch_all;

Fetch all rows into array of array.

=head2 C<fetch_first>

    my $row = $result->fetch_first;

Fetch only a first row into array and finish statment handle.

=head2 C<fetch_hash>

    my $row = $result->fetch_hash;

Fetch a row into hash

=head2 C<fetch_hash_all>

    my $rows = $result->fetch_hash_all;

Fetch all rows into array of hash.

=head2 C<fetch_hash_first>
    
    my $row = $result->fetch_hash_first;

Fetch only first row into hash and finish statment handle.

=head2 C<fetch_hash_multi>

    my $rows = $result->fetch_hash_multi(5);
    
Fetch multiple rows into array of hash
Row count must be specified.

=head2 C<fetch_multi>

    my $rows = $result->fetch_multi(5);
    
Fetch multiple rows into array of array.
Row count must be specified.

=head2 C<filter>

    $result = $result->filter(title  => 'to_something',
                              author => 'to_something');

    $result = $result->filter([qw/title author/] => 'to_something');

Filters.
These each filters override the filters applied by C<apply_filter> of
L<DBIx::Custom>.

=head2 C<one>

    my $row = $result->one;

This is alias for C<fetch_hash_first>.

=head2 C<remove_end_filter> DEPRECATED!

    $result->remove_end_filter;

Remove end filter.

=head2 C<remove_filter>

    $result->remove_filter;

Remove filter. End filter is not removed.

=head2 C<stash>

    my $stash = $result->stash;
    my $foo = $result->stash->{foo};
    $result->stash->{foo} = $foo;

Stash is hash reference to save your data.

=cut
