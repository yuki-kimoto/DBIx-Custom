package DBIx::Custom::Result;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';

# Attributes
sub sth              : Attr {}
sub fetch_filter     : Attr {}
sub no_fetch_filters      : Attr { type => 'array', trigger => sub {
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
            $row->[$i]= $fetch_filter->($keys->[$i], $row->[$i], $types->[$i],
                                        $sth, $i);
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
                  = $fetch_filter->($keys->[$i], $row->[$i],
                                    $types->[$i], $sth, $i);
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
sub fetch_first_hash {
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
sub fetch_rows_hash {
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
sub fetch_all_hash {
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

DBIx::Custom::Result - Resultset for DBIx::Custom

=head1 VERSION

Version 0.0101

=head1 SYNOPSIS

    # $result is DBIx::Custom::Result object
    my $dbi = DBIx::Custom->new;
    my $result = $dbi->query($sql_template, $param);
    
    while (my ($val1, $val2) = $result->fetch) {
        # do something
    }

=head1 OBJECT ACCESSORS

=head2 sth

    # Set and Get statement handle
    $self = $result->sth($sth);
    $sth  = $reuslt->sth

Statement handle is automatically set by DBIx::Custom.
so you do not set statement handle.

If you need statement handle, you can get statement handle by using this method.

=head2 fetch_filter

    # Set and Get fetch filter
    $self         = $result->fetch_filter($sth);
    $fetch_filter = $result->fech_filter;

Statement handle is automatically set by DBIx::Custom.
If you want to set your fetch filter, you set it.

=head2 no_fetch_filters

    # Set and Get no filter keys when fetching
    $self             = $result->no_fetch_filters($no_fetch_filters);
    $no_fetch_filters = $result->no_fetch_filters;

=head1 METHODS

=head2 fetch

    # Fetch row as array reference (Scalar context)
    $row = $result->fetch;
    
    # Fetch row as array (List context)
    @row = $result->fecth

    # Sample
    while (my $row = $result->fetch) {
        # do something
        my $val1 = $row->[0];
        my $val2 = $row->[1];
    }

fetch method is fetch resultset and get row as array or array reference.

=head2 fetch_hash

    # Fetch row as hash reference (Scalar context)
    $row = $result->fetch_hash;
    
    # Fetch row as hash (List context)
    %row = $result->fecth_hash

    # Sample
    while (my $row = $result->fetch_hash) {
        # do something
        my $val1 = $row->{key1};
        my $val2 = $row->{key2};
    }

fetch_hash method is fetch resultset and get row as hash or hash reference.

=head2 fetch_first

    # Fetch only first (Scalar context)
    $row = $result->fetch_first;
    
    # Fetch only first (List context)
    @row = $result->fetch_first;
    
This method fetch only first and finish statement handle

=head2 fetch_first_hash
    
    # Fetch only first as hash (Scalar context)
    $row = $result->fetch_first_hash;
    
    # Fetch only first as hash (Scalar context)
    @row = $result->fetch_first_hash;
    
This method fetch only first and finish statement handle

=head2 fetch_rows

    # Fetch multi rows (Scalar context)
    $rows = $result->fetch_rows($row_count);
    
    # Fetch multi rows (List context)
    @rows = $result->fetch_rows($row_count);
    
    # Sapmle 
    $rows = $result->fetch_rows(10);

=head2 fetch_rows_hash

    # Fetch multi rows as hash (Scalar context)
    $rows = $result->fetch_rows_hash($row_count);
    
    # Fetch multi rows as hash (List context)
    @rows = $result->fetch_rows_hash($row_count);
    
    # Sapmle 
    $rows = $result->fetch_rows_hash(10);

=head2 fetch_all

    # Fetch all row as array ref of array ref (Scalar context)
    $rows = $result->fetch_all;
    
    # Fetch all row as array of array ref (List context)
    @rows = $result->fecth_all;

    # Sample
    my $rows = $result->fetch_all;
    my $val0_0 = $rows->[0][0];
    my $val1_1 = $rows->[1][1];

fetch_all method is fetch resultset and get all rows as array or array reference.

=head2 fetch_all_hash

    # Fetch all row as array ref of hash ref (Scalar context)
    $rows = $result->fetch_all_hash;
    
    # Fetch all row as array of hash ref (List context)
    @rows = $result->fecth_all_hash;

    # Sample
    my $rows = $result->fetch_all_hash;
    my $val0_key1 = $rows->[0]{key1};
    my $val1_key2 = $rows->[1]{key2};

=head2 error

    # Get error infomation
    $error_messege = $result->error;
    ($error_message, $error_number, $error_state) = $result->error;

You can get get information. This is crenspond to the following.

    $error_message : $result->sth->errstr
    $error_number  : $result->sth->err
    $error_state   : $result->sth->state

=head2 finish

    # Finish statement handle
    $result->finish
    
    # Sample
    my $row = $reuslt->fetch; # fetch only one row
    $result->finish

You can finish statement handle.This is equel to

    $result->sth->finish;

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
