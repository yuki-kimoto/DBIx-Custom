use Test::More;
use strict;
use warnings;
use DBIx::Custom;

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

my $dbi;

plan skip_all => $ENV{DBIX_CUSTOM_SKIP_MESSAGE} || 'common.t is always skipped'
  unless $ENV{DBIX_CUSTOM_TEST_RUN}
    && eval { $dbi = DBIx::Custom->connect; 1 };

plan 'no_plan';

# Constant
my $create_table1 = $dbi->create_table1;

# Variable
my $model;

# Drop table
eval { $dbi->execute('drop table table1') };

# Create table
$dbi->execute($create_table1);
$model = $dbi->create_model(table => 'table1');
$model->insert({key1 => 1, key2 => 2});
is_deeply($model->select->all, [{key1 => 1, key2 => 2}]);

