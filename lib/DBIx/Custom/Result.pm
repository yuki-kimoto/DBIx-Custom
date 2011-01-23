package DBIx::Custom::Result;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr([qw/filters sth/]);

sub filter {
    my $self = shift;
    
    if (@_) {
        my $filter = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        foreach my $column (keys %$filter) {
            my $fname = $filter->{$column};
            unless (ref $fname eq 'CODE') {
              croak qq{Filter "$fname" is not registered"}
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
        my $end_filter = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        foreach my $column (keys %$end_filter) {
            my $fname = $end_filter->{$column};
            unless (ref $fname eq 'CODE') {
              croak qq{Filter "$fname" is not registered"}
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
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Filter name
        my $column = $columns->[$i];
        my $f  = exists $filter->{$column}
               ? $filter->{$column}
               : $self->default_filter;
        my $ef = $end_filter->{$column};
        
        # Filtering
        $row[$i] = $f->($row[$i]) if $f;
        $row[$i] = $ef->($row[$i]) if $ef;
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
    for (my $i = 0; $i < @$columns; $i++) {
        
        # Filter name
        my $column = $columns->[$i];
        my $f  = exists $filter->{$column}
               ? $filter->{$column}
               : $self->default_filter;
        my $ef = $end_filter->{$column};
        
        # Filtering
        $row_hash->{$column} = $f ? $f->($row->[$i]) : $row->[$i];
        $row_hash->{$column} = $ef->($row_hash->{$column}) if $ef;
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

# Deprecated
sub default_filter {
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
__PACKAGE__->attr('filter_check'); 

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

=head2 C<sth>

    my $sth = $reuslt->sth
    $result = $result->sth($sth);

Statement handle of L<DBI>.

=head1 METHODS

L<DBIx::Custom::Result> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<(experimental) end_filter>

    $result    = $result->end_filter(title  => 'to_upper_case',
                                     author => 'to_upper_case');

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

    $result    = $result->filter(title  => 'to_upper_case',
                                 author => 'to_upper_case');

Filters.
These each filters override the filters applied by C<apply_filter> of
L<DBIx::Custom>.

=cut
