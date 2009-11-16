package DBIx::Custom::ODBC;
use base 'DBIx::Custom::Basic';

use warnings;
use strict;

my $class = __PACKAGE__;

sub connect {
    my $self = shift;
    
    if (!$self->data_source && (my $database = $self->database)) {
        $self->data_source("dbi:ODBC:dbname=$database");
    }
    
    return $self->SUPER::connect;
}

=head1 NAME

DBIx::Custom::ODBC - DBIx::Custom ODBC implementation

=head1 Version

Version 0.0102

=head1 Synopsys

=head1 See DBIx::Custom and DBI::Custom::Basic documentation

This class is L<DBIx::Custom::Basic> subclass,
and L<DBIx::Custom::Basic> is L<DBIx::Custom> subclass.

You can use all methods of L<DBIx::Custom::Basic> and <DBIx::Custom>
Please see L<DBIx::Custom::Basic> and <DBIx::Custom> documentation.

=head1 Object methods

=head2 connect

    This method override DBIx::Custom::connect
    
    If database attribute is set, automatically data source is created and connect

=head1 Author

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 Copyright & license

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


