use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBI::Custom',
    license             => 'perl',
    dist_author         => 'Yuki Kimoto <kimoto.yuki@gmail.com>',
    dist_version_from   => 'lib/DBI/Custom.pm',
    build_requires => {
        'Test::More' => 0,
    },
    recommends => {
        'DBD::SQLite' => '1.25'
    },
    requires => {
        'Object::Simple' => 2.0702,
        'DBI'            => 1.605,
        'DBI::Custom::Query' => '0.0101',
        'DBI::Custom::Result' => '0.0101',
        'DBI::Custom::SQL::Template' => '0.0101'
    },
    add_to_cleanup      => [ 'DBI-Custom-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
