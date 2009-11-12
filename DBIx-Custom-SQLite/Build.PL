use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBIx::Custom::SQLite',
    license             => 'perl',
    dist_author         => 'Yuki Kimoto <kimoto.yuki@gmail.com>',
    dist_version_from   => 'lib/DBIx/Custom/SQLite.pm',
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'DBIx::Custom::Basic' => 0.0101,
        'DBD::SQLite' => 1.25
    },
    add_to_cleanup      => [ 'DBIx-Custom-SQLite-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
