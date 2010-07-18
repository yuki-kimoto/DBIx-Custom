package DBIx::Custom::Result;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';

__PACKAGE__->attr([qw/sth filters default_filter filter/]);

sub fetch {

    $_[0]->{filters} ||= {};
    $_[0]->{filter}  ||= {};
    
    # Fetch
    my @row = $_[0]->{sth}->fetchrow_array;
    
    # Cannot fetch
    return unless @row;

    # Filter
    for (my $i = 0; $i < @{$_[0]->{sth}->{NAME_lc}}; $i++) {
        my $fname  = $_[0]->{filter}->{$_[0]->{sth}->{NAME_lc}->[$i]} 
                  || $_[0]->{default_filter};
        
        croak "Filter \"$fname\" is not registered."
          if $fname && ! exists $_[0]->{filters}->{$fname};
        
        next unless $fname;
        
        $row[$i] = ref $fname
                   ? $fname->($row[$i]) 
                   : $_[0]->{filters}->{$fname}->($row[$i]);
    }

    return \@row;
}

sub fetch_first {
    my $self = shift;
    
    # Fetch
    my $row = $self->fetch;
    
    # Not exist
    return unless $row;
    
    # Finish statement handle
    $self->sth->finish;
    
    return $row;
}

sub fetch_multi {
    my ($self, $count) = @_;
    
    # Not specified Row count
    croak("Row count must be specified")
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

sub fetch_all {
    my $self = shift;
    
    # Fetch all rows
    my $rows = [];
    while(my $row = $self->fetch) {
        push @$rows, $row;
    }
    return $rows;
}

sub fetch_hash {

    $_[0]->{filters} ||= {};
    $_[0]->{filter}  ||= {};
    
    # Fetch
    my $row = $_[0]->{sth}->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;
    
    # Filter
    my $row_hash = {};
    for (my $i = 0; $i < @{$_[0]->{sth}->{NAME_lc}}; $i++) {
        
        my $fname  = $_[0]->{filter}->{$_[0]->{sth}->{NAME_lc}->[$i]}
                  || $_[0]->{default_filter};
        
        croak "Filter \"$fname\" is not registered."
          if $fname && ! exists $_[0]->{filters}->{$fname};
        
        $row_hash->{$_[0]->{sth}->{NAME_lc}->[$i]}
          = !$fname    ? $row->[$i] :
            ref $fname ? $fname->($row->[$i]) :
            $_[0]->{filters}->{$fname}->($row->[$i]);
    }
    
    return $row_hash;
}

sub fetch_hash_first {
    my $self = shift;
    
    # Fetch hash
    my $row = $self->fetch_hash;
    
    # Not exist
    return unless $row;
    
    # Finish statement handle
    $self->sth->finish;
    
    return $row;
}

sub fetch_hash_multi {
    my ($self, $count) = @_;
    
    # Not specified Row count
    croak("Row count must be specified")
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

sub fetch_hash_all {
    my $self = shift;
    
    # Fetch all rows as hash
    my $rows = [];
    while(my $row = $self->fetch_hash) {
        push @$rows, $row;
    }
    return $rows;
}

1;

=head1 NAME

DBIx::Custom::Result - Manipulate the result of select statement

=head1 SYNOPSIS
    
    # Result
    my $result = $dbi->select(table => 'books');
    
    # Fetch a row into array
    while (my $row = $result->fetch) {
        my $value1 = $row->[0];
        my $valuu2 = $row->[1];
        
        # do something
    }
    
    # Fetch only first row into array
    my $row = $result->fetch_first;
    
    # Fetch multiple rows into array of array
    while (my $rows = $result->fetch_multi(5)) {
        # do something
    }
    
    # Fetch all rows into array of array
    my $rows = $result->fetch_all;
    
    # Fetch hash into hash
    while (my $row = $result->fetch_hash) {
        my $value1 = $row->{title};
        my $value2 = $row->{author};
        
        # do something
    }
    
    # Fetch only first row into hash
    my $row = $result->fetch_hash_first;
    
    # Fetch multiple rows into array of hash
    while (my $rows = $result->fetch_hash_multi) {
        # do something
    }
    
    # Fetch all rows into array of hash
    my $rows = $result->fetch_hash_all;

=head1 ATTRIBUTES

=head2 C<sth>

    $result = $result->sth($sth);
    $sth    = $reuslt->sth

Statement handle.

=head2 C<default_filter>

    $result         = $result->default_filter('decode_utf8');
    $default_filter = $result->default_filter;

Default filter for fetching.

=head2 C<filter>

    $result = $result->filter({title => 'decode_utf8'});
    $filter = $result->filter;

Filters for fetching.

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 C<fetch>

    $row = $result->fetch;

Fetch a row into array

    while (my $row = $result->fetch) {
        # do something
        my $value1 = $row->[0];
        my $value2 = $row->[1];
    }

=head2 C<fetch_first>

    $row = $result->fetch_first;

Fetch only first row into array and finish statment handle.

=head2 C<fetch_multi>

    $rows = $result->fetch_multi($count);
    
Fetch multiple rows into array of array.

    while(my $rows = $result->fetch_multi(10)) {
        # do someting
    }

=head2 C<fetch_all>

    $rows = $result->fetch_all;

Fetch all rows into array of array.

=head2 C<fetch_hash>

    $row = $result->fetch_hash;

Fetch a row into hash

    while (my $row = $result->fetch_hash) {
        my $val1 = $row->{title};
        my $val2 = $row->{author};
        
        # do something
    }

=head2 C<fetch_hash_first>
    
    $row = $result->fetch_hash_first;

Fetch only first row into hash and finish statment handle.

=head2 C<fetch_hash_multi>

    $rows = $result->fetch_hash_multi($count);
    
Fetch multiple rows into array of hash

    while(my $rows = $result->fetch_hash_multi(10)) {
        # do someting
    }

=head2 C<fetch_hash_all>

    $rows = $result->fetch_hash_all;

Fetch all rows into array of hash.

=cut
