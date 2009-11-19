package DBIx::Custom::Result;
use Object::Simple;

use strict;
use warnings;
use Carp 'croak';

# Attributes
sub _dbi             : Attr {}
sub sth              : Attr {}
sub fetch_filter     : Attr {}

sub no_fetch_filters : Attr { type => 'array', trigger => sub {
    my $self = shift;
    my $no_fetch_filters = $self->no_fetch_filters || [];
    my %no_fetch_filters_map = map {$_ => 1} @{$no_fetch_filters};
    $self->_no_fetch_filters_map(\%no_fetch_filters_map);
}}

sub _no_fetch_filters_map : Attr {default => sub { {} }}

# Fetch (array)
sub fetch {
    my ($self, $type) = @_;
    my $sth = $self->sth;
    my $fetch_filter = $self->fetch_filter;
    
    # Fetch
    my $row = $sth->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;
    
    # Filter
    if ($fetch_filter) {
        my $keys  = $sth->{NAME_lc};
        my $types = $sth->{TYPE};
        for (my $i = 0; $i < @$keys; $i++) {
            next if $self->_no_fetch_filters_map->{$keys->[$i]};
            $row->[$i]= $fetch_filter->($row->[$i], $keys->[$i], $self->_dbi,
                                        {type => $types->[$i], sth => $sth, index => $i});
        }
    }
    return wantarray ? @$row : $row;
}

# Fetch (hash)
sub fetch_hash {
    my $self = shift;
    my $sth = $self->sth;
    my $fetch_filter = $self->fetch_filter;
    
    # Fetch
    my $row = $sth->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;
    
    # Keys
    my $keys  = $sth->{NAME_lc};
    
    # Filter
    my $row_hash = {};
    if ($fetch_filter) {
        my $types = $sth->{TYPE};
        for (my $i = 0; $i < @$keys; $i++) {
            if ($self->_no_fetch_filters_map->{$keys->[$i]}) {
                $row_hash->{$keys->[$i]} = $row->[$i];
            }
            else {
                $row_hash->{$keys->[$i]}
                  = $fetch_filter->($row->[$i], $keys->[$i], $self->_dbi,
                                    {type => $types->[$i], sth => $sth, index => $i});
            }
        }
    }
    
    # No filter
    else {
        for (my $i = 0; $i < @$keys; $i++) {
            $row_hash->{$keys->[$i]} = $row->[$i];
        }
    }
    return wantarray ? %$row_hash : $row_hash;
}

# Fetch only first (array)
sub fetch_first {
    my $self = shift;
    
    # Fetch
    my $row = $self->fetch;
    
    # Not exist
    return unless $row;
    
    # Finish statement handle
    $self->finish;
    
    return wantarray ? @$row : $row;
}

# Fetch only first (hash)
sub fetch_hash_first {
    my $self = shift;
    
    # Fetch hash
    my $row = $self->fetch_hash;
    
    # Not exist
    return unless $row;
    
    # Finish statement handle
    $self->finish;
    
    return wantarray ? %$row : $row;
}

# Fetch multi rows (array)
sub fetch_rows {
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

# Fetch multi rows (hash)
sub fetch_hash_rows {
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


# Fetch all (array)
sub fetch_all {
    my $self = shift;
    
    my $rows = [];
    while(my @row = $self->fetch) {
        push @$rows, [@row];
    }
    return wantarray ? @$rows : $rows;
}

# Fetch all (hash)
sub fetch_hash_all {
    my $self = shift;
    
    my $rows = [];
    while(my %row = $self->fetch_hash) {
        push @$rows, {%row};
    }
    return wantarray ? @$rows : $rows;
}

# Finish
sub finish { shift->sth->finish }

# Error
sub error { 
    my $self = shift;
    my $sth  = $self->sth;
    return wantarray ? ($sth->errstr, $sth->err, $sth->state) : $sth->errstr;
}

Object::Simple->build_class;

=head1 NAME

DBIx::Custom::Result - DBIx::Custom Resultset

=head1 Synopsis

    # $result is DBIx::Custom::Result object
    my $dbi = DBIx::Custom->new;
    my $result = $dbi->query($sql_template, $param);
    
    while (my ($val1, $val2) = $result->fetch) {
        # do something
    }

=head1 Accessors

=head2 sth

Set and Get statement handle

    $self = $result->sth($sth);
    $sth  = $reuslt->sth
    
    # Sample
    $dbi->sth->errstr

=head2 fetch_filter

Set and Get fetch filter

    $self         = $result->fetch_filter($sth);
    $fetch_filter = $result->fech_filter;

=head2 no_fetch_filters

Set and Get no filter keys when fetching

    $self             = $result->no_fetch_filters($no_fetch_filters);
    $no_fetch_filters = $result->no_fetch_filters;

=head1 Methods

=head2 fetch

Fetch a row

    $row = $self->fetch; # array reference
    @row = $self->fecth; # array

    # Sample
    while (my $row = $result->fetch) {
        # do something
        my $val1 = $row->[0];
        my $val2 = $row->[1];
    }

=head2 fetch_hash

Fetch row as hash

    $row = $self->fetch_hash; # hash reference
    %row = $self->fetch_hash; # hash

    # Sample
    while (my $row = $result->fetch_hash) {
        # do something
        my $val1 = $row->{key1};
        my $val2 = $row->{key2};
    }

=head2 fetch_first

Fetch only first row(Scalar context)

    $row = $self->fetch_first; # array reference
    @row = $self->fetch_first; # array
    
    # Sample
    $row = $result->fetch_first;
    
This method fetch only first row and finish statement handle

=head2 fetch_hash_first
    
Fetch only first row as hash

    $row = $self->fetch_hash_first; # hash reference
    %row = $self->fetch_hash_first; # hash
    
    # Sample
    $row = $result->fetch_hash_first;
    
This method fetch only first row and finish statement handle

=head2 fetch_rows

Fetch rows

    $rows = $self->fetch_rows($row_count); # array ref of array ref
    @rows = $self->fetch_rows($row_count); # array of array ref
    
    # Sample 
    while(my $rows = $result->fetch_rows(10)) {
        # do someting
    }

=head2 fetch_hash_rows

Fetch rows as hash

    $rows = $self->fetch_hash_rows($row_count); # array ref of hash ref
    @rows = $self->fetch_hash_rows($row_count); # array of hash ref
    
    # Sample 
    while(my $rows = $result->fetch_hash_rows(10)) {
        # do someting
    }

=head2 fetch_all

Fetch all rows

    $rows = $self->fetch_all; # array ref of array ref
    @rows = $self->fecth_all; # array of array ref

    # Sample
    my $rows = $result->fetch_all;

=head2 fetch_hash_all

Fetch all row as array ref of hash ref (Scalar context)

    $rows = $self->fetch_hash_all; # array ref of hash ref
    @rows = $self->fecth_all_hash; # array of hash ref

    # Sample
    my $rows = $result->fetch_hash_all;

=head2 error

Get error infomation

    $error_messege = $self->error;
    ($error_message, $error_number, $error_state) = $self->error;
    
    # Sample
    $error = $result->error;

You can get get information. This is same as the following.

    $error_message : $result->sth->errstr
    $error_number  : $result->sth->err
    $error_state   : $result->sth->state

=head2 finish

Finish statement handle

    $ret_val = $self->finish
    
    # Sample
    my $row = $reuslt->fetch; # fetch a row
    $result->finish

This is equel to

    $result->sth->finish;

=head1 See also

L<DBIx::Custom>

=head1 Author

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 Copyright & licence

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
