use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBIx::Custom::Basic',
    license             => 'perl',
    dist_author         => 'Yuki Kimoto <kimoto.yuki@gmail.com>',
    dist_version_from   => 'lib/DBIx/Custom/Basic.pm',
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'Encode'       => 0,
        'DBIx::Custom' => 0.0101
    },
    recommends => {
        'Time::Piece' => 1.15
    },
    add_to_cleanup      => [ 'DBIx-Custom-Basic-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
