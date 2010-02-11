package DBIx::Custom::Basic;

use warnings;
use strict;

use base 'DBIx::Custom';

use Encode qw/decode encode/;

__PACKAGE__->add_filter(
    encode_utf8 => sub { encode('UTF-8', $_[0]) },
    decode_utf8 => sub { decode('UTF-8', $_[0]) }
);

__PACKAGE__->add_format(
    'SQL99_date'        => '%Y-%m-%d',
    'SQL99_datetime'    => '%Y-%m-%d %H:%M:%S',
    'SQL99_time'        => '%H:%M:%S',
    'ISO-8601_date'     => '%Y-%m-%d',
    'ISO-8601_datetime' => '%Y-%m-%dT%H:%M:%S',
    'ISO-8601_time'     => '%H:%M:%S',
);

1;

=head1 NAME

DBIx::Custom::Basic - DBIx::Custom basic implementation

=head1 SYNOPSYS

    # New
    my $dbi = DBIx::Custom::Basic->new(
        data_source => "dbi:mysql:database=books",
        user        => 'ken',
        password    => '!LFKD%$&'
    );

=head1 METHODS

This class is L<DBIx::Custom> subclass.
You can use all methods of L<DBIx::Custom>

=head1 FILTERS

=head2 encode_utf8

Encode internal string to UTF-8 byte stream
If need, utf8::upgrade is also done.

    $dbi->filters->{encode_utf8}->($value);
    
This filter is generally used as bind filter

    $dbi->bind_filter($dbi->filters->{encode_utf8});

=head2 decode_utf8

Decode UTF-8 byte stream to internal string
    $dbi->filters->{decode_utf8}->($value);
    
This filter is generally used as fetch filter

    $dbi->fetch_filter($dbi->filters->{decode_utf8});

=head1 FORMATS
    
strptime formats is available
    
    # format name        format
    'SQL99_date'         '%Y-%m-%d',
    'SQL99_datetime'     '%Y-%m-%d %H:%M:%S',
    'SQL99_time'         '%H:%M:%S',
    'ISO-8601_date'      '%Y-%m-%d',
    'ISO-8601_datetime'  '%Y-%m-%dT%H:%M:%S',
    'ISO-8601_time'      '%H:%M:%S',

You get format as the following

    my $format = $dbi->formats->{$format_name};

=cut
