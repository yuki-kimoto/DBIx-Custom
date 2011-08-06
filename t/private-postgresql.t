use Test::More;
use strict;
use warnings;
use FindBin;
use DBIx::Custom;


$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

# Constant
my $user = 'dbix_custom';
my $password = 'dbix_custom';
my $database = 'dbix_custom';
my %connect_args_default = (
        dsn => "dbi:Pg:dbname=$database",
        user => $user,
        password => $password
);
my $create_table_default = 'create table table1 (key1 varchar(255), key2 varchar(255));';

plan skip_all => 'private test'
  unless -f "$FindBin::Bin/private-postgresql-run.tmp"
    && eval { DBIx::Custom->connect(%connect_args_default); 1 };

plan 'no_plan';


# Variable
my $dbi;
my $model;

# Connect
eval { $dbi = DBIx::Custom->connect(%connect_args_default); 1 };
ok(!$@);

# Drop table
eval { $dbi->execute('drop table table1') };

# Create table
$dbi->execute($create_table_default);
$model = $dbi->create_model(table => 'table1');
$model->insert({key1 => 1, key2 => 2});
is_deeply($model->select->all, [{key1 => 1, key2 => 2}]);



