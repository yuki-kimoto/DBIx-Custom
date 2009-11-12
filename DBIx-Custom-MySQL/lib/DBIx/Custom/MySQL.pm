package DBIx::Custom::MySQL;
use base 'DBIx::Custom::Basic';

use warnings;
use strict;
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
        $self->data_source("dbi:mysql:dbname=$database");
    }
    
    return $self->SUPER::connect;
}

=head1 NAME

DBIx::Custom::MySQL - DBIx::Custom MySQL implementation

=head1 VERSION

Version 0.0101

=head1 SYNOPSIS

    # New
    my $dbi = DBIx::Custom::MySQL->new(user => 'taro', $password => 'kliej&@K',
                                      database => 'sample_db');
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

=head1 CAUTION

This module automatically encode_utf8 or decode_utf8
If you do not want to this, you set 
    
    $dbi->bind_filter(undef);
    $dbi->fetch_filter(undef);

=head1 OBJECT METHOD

=head2 connect

    This method override DBIx::Custom::connect
    
    If database is set, automatically data source is created and connect

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


