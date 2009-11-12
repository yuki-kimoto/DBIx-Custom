use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBIx::Custom::Query',
    license             => 'perl',
    dist_author         => 'Yuki Kimoto <kimoto.yuki@gmail.com>',
    dist_version_from   => 'lib/DBIx/Custom/Query.pm',
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'Object::Simple' => 2.0702,
    },
    add_to_cleanup      => [ 'DBIx-Custom-Query-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
