use Test::More;
use strict;
use warnings;

BEGIN {
    eval { require Time::Piece; 1 }
        or plan skip_all => 'Time::Piece required';
    
    plan 'no_plan';
    use_ok('DBIx::Custom');
}

# Function for test name
my $test;
sub test {
    $test = shift;
}

# Varialbe for tests

my $format;
my $data;
my $timepiece;
my $dbi;

use DBIx::Custom::Basic;


test 'SQL99 format';
$dbi = DBIx::Custom::Basic->new;
$data   = '2009-01-02 03:04:05';
$format = $dbi->formats->{'SQL99_datetime'};
$timepiece = Time::Piece->strptime($data, $format);
is($timepiece->strftime('%F'), '2009-01-02', "$test : datetime date");
is($timepiece->strftime('%T'), '03:04:05',  "$test : datetime time");

$data   = '2009-01-02';
$format = $dbi->formats->{'SQL99_date'};
$timepiece = Time::Piece->strptime($data, $format);
is($timepiece->strftime('%F'), '2009-01-02', "$test : date");

$data   = '03:04:05';
$format = $dbi->formats->{'SQL99_time'};
$timepiece = Time::Piece->strptime($data, $format);
is($timepiece->strftime('%T'), '03:04:05',  "$test : time");


test 'ISO-8601 format';
$data   = '2009-01-02T03:04:05';
$format = $dbi->formats->{'ISO-8601_datetime'};
$timepiece = Time::Piece->strptime($data, $format);
is($timepiece->strftime('%F'), '2009-01-02', "$test : datetime date");
is($timepiece->strftime('%T'), '03:04:05',  "$test : datetime time");

$data   = '2009-01-02';
$format = $dbi->formats->{'ISO-8601_date'};
$timepiece = Time::Piece->strptime($data, $format);
is($timepiece->strftime('%F'), '2009-01-02', "$test : date");

$data   = '03:04:05';
$format = $dbi->formats->{'ISO-8601_time'};
$timepiece = Time::Piece->strptime($data, $format);
is($timepiece->strftime('%T'), '03:04:05',  "$test : time");

