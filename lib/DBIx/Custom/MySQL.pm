package DBIx::Custom::MySQL;
use base 'DBIx::Custom::Basic';

use warnings;
use strict;
use Carp 'croak';

__PACKAGE__->add_format(
    datetime => __PACKAGE__->formats->{SQL99_datetime},
    date     => __PACKAGE__->formats->{SQL99_date},
    time     => __PACKAGE__->formats->{SQL99_time},
);


sub connect {
    my $self = shift;
    
    if (!$self->data_source) {
        my $database = $self->database;
        my $host     = $self->host;
        my $port     = $self->port;
        
        my $data_source = "dbi:mysql:";
        my $data_source_original = $data_source;
        $data_source .= "database=$database;" if $database;
        $data_source .= "host=$host;"         if $host;
        $data_source .= "port=$port;"         if $port;
        
        $data_source =~ s/:$// if $data_source eq $data_source_original;
        $self->data_source($data_source);
    }
    
    return $self->SUPER::connect;
}

sub last_insert_id {
    my $self = shift;
    
    croak "Not yet connected" unless $self->connected;
    
    my $last_insert_id = $self->dbh->{mysql_insertid};
    
    return $last_insert_id;
}

=head1 NAME

DBIx::Custom::MySQL - DBIx::Custom MySQL implementation

=head1 Synopsys

    # New
    my $dbi = DBIx::Custom::MySQL->new(user => 'taro', $password => 'kliej&@K',
                                       database => 'sample_db');

=head1 See DBIx::Custom and DBIx::Custom::Basic documentation at first

This class is L<DBIx::Custom::Basic> subclass,
and L<DBIx::Custom::Basic> is L<DBIx::Custom> subclass.

You can use all methods of L<DBIx::Custom::Basic> and <DBIx::Custom>
Please see L<DBIx::Custom::Basic> and <DBIx::Custom> documentation.

=head1 Object methods

=head2 connect

Connect to database

    $self->connect;

This override L<DBIx::Custom> connect.

If you set database, host, or port, data source is automatically created.

=head2 last_insert_id

    $last_insert_id = $dbi->last_insert_id;

The folloing is last_insert_id sample.

    $dbi->insert('books', {title => 'Perl', author => 'taro'});
    $last_insert_id = $dbi->last_insert_id;

This is equal to MySQL function

    last_insert_id()
    
=head1 Author

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 Copyright & license

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


