use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'DBIx::Custom::SQL::Template',
    license             => 'perl',
    dist_author         => 'Yuki Kimoto <kimoto.yuki@gmail.com>',
    dist_version_from   => 'lib/DBIx/Custom/SQL/Template.pm',
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'Object::Simple' => 2.0702,
    },
    add_to_cleanup      => [ 'DBIx-Custom-SQL-Template-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
