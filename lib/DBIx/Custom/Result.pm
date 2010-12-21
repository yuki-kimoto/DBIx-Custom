package DBIx::Custom::Result;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr([qw/filter_check filters sth/]);

sub default_filter {
    my $self = shift;
    my $fname = $_[0];
    
    if (@_ && !$fname) {
        $self->{_default_filter} = undef;
    }
    else {
        croak qq{"$fname" is not registered}
          unless exists $self->filters->{$fname};
    
        $self->{_default_filter} = $self->filters->{$fname};
    }
    
    return $self;
}

sub filter {
    my $self = shift;
    my $filter = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    foreach my $column (keys %$filter) {
        my $fname = $filter->{$column};
        unless (ref $fname eq 'CODE') {
          croak qq{"$fname" is not registered"}
            unless exists $self->filters->{$fname};
          
          $filter->{$column} = $self->filters->{$fname};
        }
    }
    
    $self->{_filter} = $filter;
    
    return $self;
}

sub fetch {
    my $self = shift;
    
    # Filters
    my $filters = $self->{filters} || {};
    my $filter  = $self->{_filter}  || {};
    my $auto_filter = $self->{_auto_filter} || {};
    $filter = {%$auto_filter, %$filter};
    
    # Fetch
    my @row = $self->{sth}->fetchrow_array;
    
    # No row
    return unless @row;
    
    # Filtering
    my $columns = $self->{sth}->{NAME_lc};
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Filter name
        my $column = $columns->[$i];
        my $f  = exists $filter->{$column}
               ? $filter->{$column}
               : $self->{_default_filter};
        
        # Filtering
        $row[$i] = $f->($row[$i]) if $f;
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
    
    # Filters
    my $filters = $self->{filters} || {};
    my $filter  = $self->{_filter}  || {};
    my $auto_filter = $self->{_auto_filter} || {};
    $filter = {%$auto_filter, %$filter};
    
    # Fetch
    my $row = $self->{sth}->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;

    # Filter
    my $row_hash = {};
    my $columns = $self->{sth}->{NAME_lc};
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Filter name
        my $column = $columns->[$i];
        my $f  = exists $filter->{$column}
               ? $filter->{$column}
               : $self->{_default_filter};
        
        # Filtering
        $row_hash->{$column} = $f ? $f->($row->[$i]) : $row->[$i];
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
    croak 'Row count must be specified'
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
    croak 'Row count must be specified'
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

=head2 C<filters>

    my $filters = $result->filters;
    $result     = $result->filters(\%filters);

Resistered filters.

=head2 C<filter_check>

    my $filter_check = $result->filter_check;
    $result          = $result->filter_check;

Enable filter validation.

=head2 C<sth>

    my $sth = $reuslt->sth
    $result = $result->sth($sth);

Statement handle of L<DBI>.

=head1 METHODS

L<DBIx::Custom::Result> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<(deprecated) default_filter>

    $result = $result->default_filter($filter);

Default filter when a row is fetched.

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

    $result    = $result->filter(title  => 'decode_utf8',
                                 author => 'decode_utf8');

=cut
