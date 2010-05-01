package DBIx::Custom::Result;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr([qw/sth filters default_filter filter/]);

sub fetch {
    my ($self, $type) = @_;
    
    my $sth            = $self->sth;
    my $filters        = $self->filters || {};
    my $default_filter = $self->default_filter || '';
    my $filter         = $self->filter || {};
    
    # Fetch
    my $row = $sth->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;

    # Key
    my $columns  = $sth->{NAME_lc};
    
    # Filter
    for (my $i = 0; $i < @$columns; $i++) {
        my $fname  = $filter->{$columns->[$i]} || $filters->{$default_filter} || '';
        my $filter = $filters->{$fname};
        $row->[$i] = $filter->($row->[$i]) if $filter;
    }

    return wantarray ? @$row : $row;
}

sub fetch_hash {
    my $self = shift;

    my $sth            = $self->sth;
    my $filters        = $self->filters || {};
    my $default_filter = $self->default_filter || '';
    my $filter         = $self->filter || {};
    
    # Fetch
    my $row = $sth->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;
    
    # Keys
    my $columns  = $sth->{NAME_lc};
    
    # Filter
    my $row_hash = {};
    for (my $i = 0; $i < @$columns; $i++) {
        my $fname  = $filter->{$columns->[$i]} || $default_filter || '';
        my $filter = $filters->{$fname};
        $row_hash->{$columns->[$i]} = $filter
                                    ? $filter->($row->[$i])
                                    : $row->[$i];
    }
    
    return wantarray ? %$row_hash : $row_hash;
}

sub fetch_single {
    my $self = shift;
    
    # Fetch
    my $row = $self->fetch;
    
    # Not exist
    return unless $row;
    
    # Finish statement handle
    $self->finish;
    
    return wantarray ? @$row : $row;
}

sub fetch_hash_single {
    my $self = shift;
    
    # Fetch hash
    my $row = $self->fetch_hash;
    
    # Not exist
    return unless $row;
    
    # Finish statement handle
    $self->finish;
    
    return wantarray ? %$row : $row;
}

sub fetch_multi {
    my ($self, $count) = @_;
    
    # Not specified Row count
    croak("Row count must be specified")
      unless $count;
    
    # Fetch multi rows
    my $rows = [];
    for (my $i = 0; $i < $count; $i++) {
        my @row = $self->fetch;
        
        last unless @row;
        
        push @$rows, \@row;
    }
    
    return unless @$rows;
    return wantarray ? @$rows : $rows;
}

sub fetch_hash_multi {
    my ($self, $count) = @_;
    
    # Not specified Row count
    croak("Row count must be specified")
      unless $count;
    
    # Fetch multi rows
    my $rows = [];
    for (my $i = 0; $i < $count; $i++) {
        my %row = $self->fetch_hash;
        
        last unless %row;
        
        push @$rows, \%row;
    }
    
    return unless @$rows;
    return wantarray ? @$rows : $rows;
}

sub fetch_all {
    my $self = shift;
    
    # Fetch all rows
    my $rows = [];
    while(my @row = $self->fetch) {
        push @$rows, [@row];
    }
    return wantarray ? @$rows : $rows;
}

sub fetch_hash_all {
    my $self = shift;
    
    # Fetch all rows as hash
    my $rows = [];
    while(my %row = $self->fetch_hash) {
        push @$rows, {%row};
    }
    return wantarray ? @$rows : $rows;
}

sub finish { shift->sth->finish }

sub error { 
    my $self = shift;
    
    # Statement handle
    my $sth  = $self->sth;
    
    return wantarray ? ($sth->errstr, $sth->err, $sth->state) : $sth->errstr;
}

1;

=head1 NAME

DBIx::Custom::Result - DBIx::Custom Resultset

=head1 SYNOPSIS

    my $result = $dbi->execute($query);
    
    # Fetch
    while (my @row = $result->fetch) {
        # Do something
    }
    
    # Fetch hash
    while (my %row = $result->fetch_hash) {
        # Do something
    }

=head1 ATTRIBUTES

=head2 sth

Statement handle

    $result = $result->sth($sth);
    $sth    = $reuslt->sth
    
=head2 default_filter

Filter excuted when data is fetched

    $result         = $result->default_filter($default_filter);
    $default_filter = $result->default_filter;

=head2 filter

Filter excuted when data is fetched

    $result   = $result->filter($sth);
    $filter   = $result->filter;

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 new

    my $result = DBIx::Custom::Result->new;

=head2 fetch

Fetch a row

    $row = $result->fetch; # array reference
    @row = $result->fecth; # array

The following is fetch sample

    while (my $row = $result->fetch) {
        # do something
        my $val1 = $row->[0];
        my $val2 = $row->[1];
    }

=head2 fetch_hash

Fetch row as hash

    $row = $result->fetch_hash; # hash reference
    %row = $result->fetch_hash; # hash

The following is fetch_hash sample

    while (my $row = $result->fetch_hash) {
        # do something
        my $val1 = $row->{key1};
        my $val2 = $row->{key2};
    }

=head2 fetch_single

Fetch only first row(Scalar context)

    $row = $result->fetch_single; # array reference
    @row = $result->fetch_single; # array
    
The following is fetch_single sample

    $row = $result->fetch_single;
    
This method fetch only first row and finish statement handle

=head2 fetch_hash_single
    
Fetch only first row as hash

    $row = $result->fetch_hash_single; # hash reference
    %row = $result->fetch_hash_single; # hash
    
The following is fetch_hash_single sample

    $row = $result->fetch_hash_single;
    
This method fetch only single row and finish statement handle

=head2 fetch_multi

Fetch rows

    $rows = $result->fetch_multi($row_count); # array ref of array ref
    @rows = $result->fetch_multi($row_count); # array of array ref
    
The following is fetch_multi sample

    while(my $rows = $result->fetch_multi(10)) {
        # do someting
    }

=head2 fetch_hash_multi

Fetch rows as hash

    $rows = $result->fetch_hash_multi($row_count); # array ref of hash ref
    @rows = $result->fetch_hash_multi($row_count); # array of hash ref
    
The following is fetch_hash_multi sample

    while(my $rows = $result->fetch_hash_multi(10)) {
        # do someting
    }

=head2 fetch_all

Fetch all rows

    $rows = $result->fetch_all; # array ref of array ref
    @rows = $result->fecth_all; # array of array ref

The following is fetch_all sample

    my $rows = $result->fetch_all;

=head2 fetch_hash_all

Fetch all row as array ref of hash ref (Scalar context)

    $rows = $result->fetch_hash_all; # array ref of hash ref
    @rows = $result->fecth_all_hash; # array of hash ref

The following is fetch_hash_all sample

    my $rows = $result->fetch_hash_all;

=head2 error

Get error infomation

    $error_messege = $result->error;
    ($error_message, $error_number, $error_state) = $result->error;
    

You can get get information. This is same as the following.

    $error_message : $result->sth->errstr
    $error_number  : $result->sth->err
    $error_state   : $result->sth->state

=head2 finish

Finish statement handle

    $result->finish

This is equel to

    $result->sth->finish;

=cut
