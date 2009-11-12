package DBIx::Custom::SQLite;
use base 'DBIx::Custom::Basic';

use warnings;
use strict;
use Carp 'croak';

our $VERSION = '0.0101';

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

=head1 VERSION

Version 0.0101

=head1 SYNOPSYS

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
    
=head1 CAUTION

This module automatically encode_utf8 or decode_utf8
If you do not want to this, you set 
    
    $dbi->bind_filter(undef);
    $dbi->fetch_filter(undef);

=head1 OBJECT METHOD

=head2 connect

    This method override DBIx::Custom::connect
    
    If database is set, automatically data source is created and connect

=head2 connect_memory

    # Connect memory database
    $self = $dbi->connect_memory;

=head2 reconnect_memory

    # Reconnect memory database
    $self = $dbi->reconnect_memory;

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

