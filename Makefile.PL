use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'DBIx::Custom',
    AUTHOR              => 'Yuki Kimoto <kimoto.yuki@gmail.com>',
    VERSION_FROM        => 'lib/DBIx/Custom.pm',
    ABSTRACT_FROM       => 'lib/DBIx/Custom.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
      PREREQ_PM => {
        'Test::More' => 0,
        'Object::Simple' => 3.0621,
        'DBD::SQLite' => '1.25',
        'DBI' => '1.605'
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-Custom-*' },
    test => {TESTS => 't/*.t t/next/*.t'}
);