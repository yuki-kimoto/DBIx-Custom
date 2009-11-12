package DBIx::Custom::SQLite;
use base 'DBIx::Custom::Basic';

use warnings;
use strict;
use Carp 'croak';

our $VERSION = '0.0102';

my $class = __PACKAGE__;

$class->add_format(
    datetime => $class->formats->{SQL99_datetime},
    date     => $class->formats->{SQL99_date},
    time     => $class->formats->{SQL99_time},
);

sub connect {
    my $self = shift;
    
    if (!$self->data_source && (my $database = $self->database)) {
        $self->data_source("dbi:SQLite:dbname=$database");
    }
    
    return $self->SUPER::connect;
}

sub connect_memory {
    my $self = shift;
    
    # Data source for memory database
    $self->data_source('dbi:SQLite:dbname=:memory:');
    
    # Already connected
    croak("Already connected") if $self->connected;
    
    # Connect
    $self->connect;
    
    return $self;
}

sub reconnect_memory {
    my $self = shift;

    # Data source for memory database
    $self->data_source('dbi:SQLite:dbname=:memory:');
    
    # Reconnect
    $self->reconnect;
    
    return $self;
}


=head1 NAME

DBIx::Custom::SQLite - DBIx::Custom SQLite implementation

=head1 Version

Version 0.0102

=head1 Synopsys

    use DBIx::Custom::SQLite;
    
    # New
    my $dbi = DBIx::Custom::SQLite->new(user => 'taro', $password => 'kliej&@K',
                                       database => 'sample.db');
    
    # Insert 
    $dbi->insert('books', {title => 'perl', author => 'taro'});
    
    # Update 
    # same as 'update books set (title = 'aaa', author = 'ken') where id = 5;
    $dbi->update('books', {title => 'aaa', author => 'ken'}, {id => 5});
    
    # Delete
    $dbi->delete('books', {author => 'taro'});
    
    # select * from books;
    $dbi->select('books');
    
    # select * from books where ahthor = 'taro'; 
    $dbi->select('books', {author => 'taro'}); 
    
    # select author, title from books where author = 'taro'
    $dbi->select('books', [qw/author title/], {author => 'taro'});
    
    # select author, title from books where author = 'taro' order by id limit 1;
    $dbi->select('books', [qw/author title/], {author => 'taro'},
                 'order by id limit 1');

=head1 See DBIx::Custom and DBI::Custom::Basic documentation

This class is L<DBIx::Custom::Basic> subclass.
and L<DBIx::Custom::Basic> is L<DBIx::Custom> subclass

You can use all methods of L<DBIx::Custom::Basic> and <DBIx::Custom>
Please see L<DBIx::Custom::Basic> and <DBIx::Custom> documentation

=head1 Object methods

=head2 connect

This override L<DBIx::Custom> connect.

    # Connect to database
    $dbi->connect;

If database attribute is set, automatically data source is created and connect

=head2 connect_memory

    # Connect memory database
    $self = $dbi->connect_memory;

=head2 reconnect_memory

    # Reconnect memory database
    $self = $dbi->reconnect_memory;

=head1 Author

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 Copyright & lisence

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

