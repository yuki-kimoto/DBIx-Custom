package DBIx::Custom::Pg;
use base 'DBIx::Custom::Basic';

use warnings;
use strict;

my $class = __PACKAGE__;


sub connect {
    my $self = shift;
    
    if (!$self->data_source) {
        my $database = $self->database;
        my $host     = $self->host;
        my $port     = $self->port;
        
        my $data_source = "dbi:Pg:";
        my $data_source_original = $data_source;
        $data_source .= "dbname=$database;" if $database;
        $data_source .= "host=$host;"       if $host;
        $data_source .= "port=$port;"       if $port;
        
        $data_source =~ s/:$// if $data_source eq $data_source_original;
        $self->data_source($data_source);
    }
    
    return $self->SUPER::connect;
}

=head1 NAME

DBIx::Custom::Pg - DBIx::Custom PostgreSQL implementation

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

=head2 last_insert_id

=head1 Author

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 Copyright & license

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


