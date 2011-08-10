use Test::More;
use strict;
use warnings;
use utf8;
use Encode qw/encode_utf8 decode_utf8/;
use FindBin;
use lib "$FindBin::Bin/common";

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1.25 }
        or plan skip_all => 'DBD::SQLite >= 1.25 required';

    plan 'no_plan';
    use_ok('DBIx::Custom');
}

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};
sub test { print "# $_[0]\n" }

use DBIx::Custom;
{
    package DBIx::Custom;
    has dsn => sub { 'dbi:SQLite:dbname=:memory:' }
}
use MyDBI1;
{
    package MyDBI4;

    use strict;
    use warnings;

    use base 'DBIx::Custom';

    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model(
            MyModel2 => [
                'book',
                {class => 'Company', name => 'company'}
            ]
        );
    }

    package MyModel2::Base1;

    use strict;
    use warnings;

    use base 'DBIx::Custom::Model';

    package MyModel2::book;

    use strict;
    use warnings;

    use base 'MyModel2::Base1';

    sub insert {
        my ($self, $param) = @_;
        
        return $self->SUPER::insert(param => $param);
    }

    sub list { shift->select; }

    package MyModel2::Company;

    use strict;
    use warnings;

    use base 'MyModel2::Base1';

    sub insert {
        my ($self, $param) = @_;
        
        return $self->SUPER::insert(param => $param);
    }

    sub list { shift->select; }
}
{
     package MyDBI5;

    use strict;
    use warnings;

    use base 'DBIx::Custom';

    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel4');
    }
}
{
    package MyDBI6;
    
    use base 'DBIx::Custom';
    
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel5');
        
        return $self;
    }
}
{
    package MyDBI7;
    
    use base 'DBIx::Custom';
    
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel6');
        
        
        return $self;
    }
}
{
    package MyDBI8;
    
    use base 'DBIx::Custom';
    
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel7');
        
        return $self;
    }
}

{
    package MyDBI9;
    
    use base 'DBIx::Custom';
    
    sub connect {
        my $self = shift->SUPER::connect(@_);
        
        $self->include_model('MyModel8')->setup_model;
        
        return $self;
    }
}

# Constant
my $create_table1 = 'create table table1 (key1 char(255), key2 char(255));';
my $create_table1_2 = 'create table table1 (key1 char(255), key2 char(255), key3 char(255), key4 char(255), key5 char(255));';
my $create_table2 = 'create table table2 (key1 char(255), key3 char(255));';
my $create_table2_2 = "create table table2 (key1, key2, key3)";
my $create_table3 = "create table table3 (key1, key2, key3)";
my $create_table_reserved = 'create table "table" ("select", "update")';

my $q = '"';
my $p = '"';

# Variables
my $builder;
my $datas;
my $dbi;
my $sth;
my $source;
my @sources;
my $select_source;
my $insert_source;
my $update_source;
my $param;
my $params;
my $sql;
my $result;
my $row;
my @rows;
my $rows;
my $query;
my @queries;
my $select_query;
my $insert_query;
my $update_query;
my $ret_val;
my $infos;
my $model;
my $model2;
my $where;
my $update_param;
my $insert_param;
my $join;
my $binary;

# Prepare table
$dbi = DBIx::Custom->connect;


test 'delete_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->delete_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2],
);
is_deeply($dbi->select(table => 'table1')->all, []);

$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->delete_at(
    table => 'table1',
    primary_key => 'key1',
    where => 1,
);
is_deeply($dbi->select(table => 'table1')->all, []);

test 'insert_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert_at(
    primary_key => ['key1', 'key2'], 
    table => 'table1',
    where => [1, 2],
    param => {key3 => 3}
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->insert_at(
    primary_key => 'key1', 
    table => 'table1',
    where => 1,
    param => {key2 => 2, key3 => 3}
);

is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 3);

eval {
    $dbi->insert_at(
        table => 'table1',
        primary_key => ['key1', 'key2'],
        where => {},
        param => {key1 => 1, key2 => 2, key3 => 3},
    );
};
like($@, qr/must be/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert_at(
    {key3 => 3},
    primary_key => ['key1', 'key2'], 
    table => 'table1',
    where => [1, 2],
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 3);

test 'update_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2],
    param => {key3 => 4}
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 4);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update_at(
    table => 'table1',
    primary_key => 'key1',
    where => 1,
    param => {key3 => 4}
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 4);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update_at(
    {key3 => 4},
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2]
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 4);

test 'select_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2]
);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select_at(
    table => 'table1',
    primary_key => 'key1',
    where => 1,
);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select_at(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    where => [1, 2]
);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

eval {
    $result = $dbi->select_at(
        table => 'table1',
        primary_key => ['key1', 'key2'],
        where => {},
    );
};
like($@, qr/must be/);

eval {
    $result = $dbi->select_at(
        table => 'table1',
        primary_key => ['key1', 'key2'],
        where => [1],
    );
};
like($@, qr/same/);

eval {
    $result = $dbi->update_at(
        table => 'table1',
        primary_key => ['key1', 'key2'],
        where => {},
        param => {key1 => 1, key2 => 2},
    );
};
like($@, qr/must be/);

eval {
    $result = $dbi->delete_at(
        table => 'table1',
        primary_key => ['key1', 'key2'],
        where => {},
    );
};
like($@, qr/must be/);

test 'columns';
use MyDBI1;
$dbi = MyDBI1->connect;
$model = $dbi->model('book');


test 'model delete_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
eval { $dbi->execute('drop table table3') };
$dbi->execute($create_table1_2);
$dbi->execute($create_table2_2);
$dbi->execute($create_table3);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1')->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => 'table1')->all, []);
$dbi->insert(table => 'table2', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1_1')->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => 'table1')->all, []);
$dbi->insert(table => 'table3', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1_3')->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => 'table1')->all, []);

test 'model insert_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->model('table1')->insert_at(
    where => [1, 2],
    param => {key3 => 3}
);
$result = $dbi->model('table1')->select;
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

test 'model update_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1')->update_at(
    where => [1, 2],
    param => {key3 => 4}
);
$result = $dbi->model('table1')->select;
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 4);

test 'model select_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->model('table1')->select_at(where => [1, 2]);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);


test 'mycolumn and column';
$dbi = MyDBI7->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->separator('__');
$dbi->setup_model;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->model('table1');
$result = $model->select(
    column => [$model->mycolumn, $model->column('table2')],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {key1 => 1, key2 => 2, 'table2__key1' => 1, 'table2__key3' => 3});

test 'update_param';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$param = {key2 => 11};
$update_param = $dbi->update_param($param);
$sql = <<"EOS";
update table1 $update_param
where key1 = 1
EOS
$dbi->execute($sql, param => $param);
$result = $dbi->execute('select * from table1;', table => 'table1');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");


$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$param = {key2 => 11, key3 => 33};
$update_param = $dbi->update_param($param);
$sql = <<"EOS";
update table1 $update_param
where key1 = 1
EOS
$dbi->execute($sql, param => $param);
$result = $dbi->execute('select * from table1;', table => 'table1');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 33, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$param = {key2 => 11, key3 => 33};
$update_param = $dbi->update_param($param, {no_set => 1});
$sql = <<"EOS";
update table1 set $update_param
where key1 = 1
EOS
$dbi->execute($sql, param => $param);
$result = $dbi->execute('select * from table1;', table => 'table1');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 33, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "update param no_set");

            
eval { $dbi->update_param({";" => 1}) };
like($@, qr/not safety/);


test 'update_param';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$param = {key2 => 11};
$update_param = $dbi->assign_param($param);
$sql = <<"EOS";
update table1 set $update_param
where key1 = 1
EOS
$dbi->execute($sql, param => $param, table => 'table1');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");


test 'insert_param';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$param = {key1 => 1, key2 => 2};
$insert_param = $dbi->insert_param($param);
$sql = <<"EOS";
insert into table1 $insert_param
EOS
$dbi->execute($sql, param => $param, table => 'table1');
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);

$dbi = DBIx::Custom->connect;
$dbi->quote('"');
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$param = {key1 => 1, key2 => 2};
$insert_param = $dbi->insert_param($param);
$sql = <<"EOS";
insert into table1 $insert_param
EOS
$dbi->execute($sql, param => $param, table => 'table1');
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);

eval { $dbi->insert_param({";" => 1}) };
like($@, qr/not safety/);


test 'join';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$dbi->execute('create table table3 (key3 int, key4 int);');
$dbi->insert(table => 'table3', param => {key3 => 5, key4 => 4});
$rows = $dbi->select(
    table => 'table1',
    column => 'table1.key1 as table1_key1, table2.key1 as table2_key1, key2, key3',
    where   => {'table1.key2' => 2},
    join  => ['left outer join table2 on table1.key1 = table2.key1']
)->all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}]);

$rows = $dbi->select(
    table => 'table1',
    where   => {'key1' => 1},
    join  => ['left outer join table2 on table1.key1 = table2.key1']
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

eval {
    $rows = $dbi->select(
        table => 'table1',
        column => 'table1.key1 as table1_key1, table2.key1 as table2_key1, key2, key3',
        where   => {'table1.key2' => 2},
        join  => {'table1.key1' => 'table2.key1'}
    );
};
like ($@, qr/array/);

$rows = $dbi->select(
    table => 'table1',
    where   => {'key1' => 1},
    join  => ['left outer join table2 on table1.key1 = table2.key1',
              'left outer join table3 on table2.key3 = table3.key3']
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

$rows = $dbi->select(
    column => 'table3.key4 as table3__key4',
    table => 'table1',
    where   => {'table1.key1' => 1},
    join  => ['left outer join table2 on table1.key1 = table2.key1',
              'left outer join table3 on table2.key3 = table3.key3']
)->all;
is_deeply($rows, [{table3__key4 => 4}]);

$rows = $dbi->select(
    column => 'table1.key1 as table1__key1',
    table => 'table1',
    where   => {'table3.key4' => 4},
    join  => ['left outer join table2 on table1.key1 = table2.key1',
              'left outer join table3 on table2.key3 = table3.key3']
)->all;
is_deeply($rows, [{table1__key1 => 1}]);

$dbi = DBIx::Custom->connect;
$dbi->quote('"');
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$rows = $dbi->select(
    table => 'table1',
    column => '"table1"."key1" as "table1_key1", "table2"."key1" as "table2_key1", "key2", "key3"',
    where   => {'table1.key2' => 2},
    join  => ['left outer join "table2" on "table1"."key1" = "table2"."key1"'],
)->all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}],
          'quote');


$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$sql = <<"EOS";
left outer join (
  select * from table1 as t1
  where t1.key2 = (
    select max(t2.key2) from table1 as t2
    where t1.key1 = t2.key1
  )
) as latest_table1 on table1.key1 = latest_table1.key1
EOS
$join = [$sql];
$rows = $dbi->select(
    table => 'table1',
    column => 'latest_table1.key1 as latest_table1__key1',
    join  => $join
)->all;
is_deeply($rows, [{latest_table1__key1 => 1}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 4});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$result = $dbi->select(
    table => 'table1',
    join => [
        "left outer join table2 on table2.key2 = '4' and table1.key1 = table2.key1"
    ]
);
is_deeply($result->all, [{key1 => 1, key2 => 2}]);
$result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
        "left outer join table2 on table2.key3 = '4' and table1.key1 = table2.key1"
    ]
);
is_deeply($result->all, [{'table2.key3' => 4}]);
$result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
        "left outer join table2 on table1.key1 = table2.key1 and table2.key3 = '4'"
    ]
);
is_deeply($result->all, [{'table2.key3' => 4}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 4});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$result = $dbi->select(
    table => 'table1',
    column => [{table2 => ['key3']}],
    join => [
        {
            clause => "left outer join table2 on table2.key3 = '4' and table1.key1 = table2.key1",
            table => ['table1', 'table2']
        }
    ]
);
is_deeply($result->all, [{'table2.key3' => 4}]);

test 'mycolumn';
$dbi = MyDBI8->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->model('table1');
$result = $model->select_at(
    column => [
        $model->mycolumn,
        $model->column('table2')
    ]
);
is_deeply($result->one,
          {key1 => 1, key2 => 2, 'table2.key1' => 1, 'table2.key3' => 3});

$result = $model->select_at(
    column => [
        $model->mycolumn(['key1']),
        $model->column(table2 => ['key1'])
    ]
);
is_deeply($result->one,
          {key1 => 1, 'table2.key1' => 1});
$result = $model->select_at(
    column => [
        $model->mycolumn(['key1']),
        {table2 => ['key1']}
    ]
);
is_deeply($result->one,
          {key1 => 1, 'table2.key1' => 1});

$result = $model->select_at(
    column => [
        $model->mycolumn(['key1']),
        ['table2.key1', as => 'table2.key1']
    ]
);
is_deeply($result->one,
          {key1 => 1, 'table2.key1' => 1});

$result = $model->select_at(
    column => [
        $model->mycolumn(['key1']),
        ['table2.key1' => 'table2.key1']
    ]
);
is_deeply($result->one,
          {key1 => 1, 'table2.key1' => 1});

test 'dbi method from model';
$dbi = MyDBI9->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$model = $dbi->model('table1');
eval{$model->execute('select * from table1')};
ok(!$@);

test 'column table option';
$dbi = MyDBI9->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
$dbi->execute('insert into table1 (key1, key2) values (1, 2);');
$dbi->execute('insert into table2 (key1, key3) values (1, 4);');
$model = $dbi->model('table1');
$result = $model->select(
    column => [
        $model->column('table2', {alias => 'table2_alias'})
    ],
    where => {'table2_alias.key3' => 4}
);
is_deeply($result->one, 
          {'table2_alias.key1' => 1, 'table2_alias.key3' => 4});

$dbi->separator('__');
$result = $model->select(
    column => [
        $model->column('table2', {alias => 'table2_alias'})
    ],
    where => {'table2_alias.key3' => 4}
);
is_deeply($result->one, 
          {'table2_alias__key1' => 1, 'table2_alias__key3' => 4});

$dbi->separator('-');
$result = $model->select(
    column => [
        $model->column('table2', {alias => 'table2_alias'})
    ],
    where => {'table2_alias.key3' => 4}
);
is_deeply($result->one, 
          {'table2_alias-key1' => 1, 'table2_alias-key3' => 4});

test 'type option'; # DEPRECATED!
$dbi = DBIx::Custom->connect(
    dbi_option => {
        $DBD::SQLite::VERSION > 1.26 ? (sqlite_unicode => 1) : (unicode => 1)
    }
);
$binary = pack("I3", 1, 2, 3);
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1(key1, key2)');
$dbi->insert(table => 'table1', param => {key1 => $binary, key2 => 'あ'}, type => [key1 => DBI::SQL_BLOB]);
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);

$dbi->insert(table => 'table1', param => {key1 => $binary, key2 => 'あ'}, type => [['key1'] => DBI::SQL_BLOB]);
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);


test 'bind_type option';
$dbi = DBIx::Custom->connect(
    dbi_option => {
        $DBD::SQLite::VERSION > 1.26 ? (sqlite_unicode => 1) : (unicode => 1)
    }
);
$binary = pack("I3", 1, 2, 3);
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1(key1, key2)');
$dbi->insert(table => 'table1', param => {key1 => $binary, key2 => 'あ'}, bind_type => [key1 => DBI::SQL_BLOB]);
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);

$dbi->insert(table => 'table1', param => {key1 => $binary, key2 => 'あ'}, bind_type => [['key1'] => DBI::SQL_BLOB]);
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);

test 'model type attribute';
$dbi = DBIx::Custom->connect(
    dbi_option => {
        $DBD::SQLite::VERSION > 1.26 ? (sqlite_unicode => 1) : (unicode => 1)
    }
);
$binary = pack("I3", 1, 2, 3);
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1(key1, key2)');
$model = $dbi->create_model(table => 'table1', bind_type => [key1 => DBI::SQL_BLOB]);
$model->insert(param => {key1 => $binary, key2 => 'あ'});
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);

test 'create_model';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);

$dbi->create_model(
    table => 'table1',
    join => [
       'left outer join table2 on table1.key1 = table2.key1'
    ],
    primary_key => ['key1']
);
$model2 = $dbi->create_model(
    table => 'table2'
);
$dbi->create_model(
    table => 'table3',
    filter => [
        key1 => {in => sub { uc $_[0] }}
    ]
);
$dbi->setup_model;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->model('table1');
$result = $model->select(
    column => [$model->mycolumn, $model->column('table2')],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {key1 => 1, key2 => 2, 'table2.key1' => 1, 'table2.key3' => 3});
is_deeply($model2->select->one, {key1 => 1, key3 => 3});

test 'model method';
test 'create_model';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->create_model(
    table => 'table2'
);
$model->method(foo => sub { shift->select(@_) });
is_deeply($model->foo->one, {key1 => 1, key3 => 3});

test 'merge_param';
$dbi = DBIx::Custom->new;
$params = [
    {key1 => 1, key2 => 2, key3 => 3},
    {key1 => 1, key2 => 2},
    {key1 => 1}
];
$param = $dbi->merge_param($params->[0], $params->[1], $params->[2]);
is_deeply($param, {key1 => [1, 1, 1], key2 => [2, 2], key3 => 3});

$params = [
    {key1 => [1, 2], key2 => 1, key3 => [1, 2]},
    {key1 => [3, 4], key2 => [2, 3], key3 => 3}
];
$param = $dbi->merge_param($params->[0], $params->[1]);
is_deeply($param, {key1 => [1, 2, 3, 4], key2 => [1, 2, 3], key3 => [1, 2, 3]});

test 'select() param option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 4});
$dbi->insert(table => 'table2', param => {key1 => 2, key3 => 5});
$rows = $dbi->select(
    table => 'table1',
    column => 'table1.key1 as table1_key1, key2, key3',
    where   => {'table1.key2' => 3},
    join  => ['inner join (select * from table2 where {= table2.key3})' . 
              ' as table2 on table1.key1 = table2.key1'],
    param => {'table2.key3' => 5}
)->all;
is_deeply($rows, [{table1_key1 => 2, key2 => 3, key3 => 5}]);


test 'select() wrap option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$rows = $dbi->select(
    table => 'table1',
    column => 'key1',
    wrap => ['select * from (', ') as t where key1 = 1']
)->all;
is_deeply($rows, [{key1 => 1}]);

eval {
$dbi->select(
    table => 'table1',
    column => 'key1',
    wrap => 'select * from ('
)
};
like($@, qr/array/);

test 'select() string where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$rows = $dbi->select(
    table => 'table1',
    where => 'key1 = :key1 and key2 = :key2',
    where_param => {key1 => 1, key2 => 2}
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$rows = $dbi->select(
    table => 'table1',
    where => [
        'key1 = :key1 and key2 = :key2',
        {key1 => 1, key2 => 2}
    ]
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

test 'delete() string where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$dbi->delete(
    table => 'table1',
    where => 'key1 = :key1 and key2 = :key2',
    where_param => {key1 => 1, key2 => 2}
);
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 2, key2 => 3}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$dbi->delete(
    table => 'table1',
    where => [
        'key1 = :key1 and key2 = :key2',
         {key1 => 1, key2 => 2}
    ]
);
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 2, key2 => 3}]);


test 'update() string where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->update(
    table => 'table1',
    param => {key1 => 5},
    where => 'key1 = :key1 and key2 = :key2',
    where_param => {key1 => 1, key2 => 2}
);
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 5, key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->update(
    table => 'table1',
    param => {key1 => 5},
    where => [
        'key1 = :key1 and key2 = :key2',
        {key1 => 1, key2 => 2}
    ]
);
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 5, key2 => 2}]);

test 'insert id and primary_key option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(
    primary_key => ['key1', 'key2'], 
    table => 'table1',
    id => [1, 2],
    param => {key3 => 3}
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(
    primary_key => 'key1', 
    table => 'table1',
    id => 0,
    param => {key2 => 2, key3 => 3}
);

is($dbi->select(table => 'table1')->one->{key1}, 0);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 3);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(
    {key3 => 3},
    primary_key => ['key1', 'key2'], 
    table => 'table1',
    id => [1, 2],
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 3);


test 'model insert id and primary_key option';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->model('table1')->insert(
    id => [1, 2],
    param => {key3 => 3}
);
$result = $dbi->model('table1')->select;
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->model('table1')->insert(
    {key3 => 3},
    id => [1, 2]
);
$result = $dbi->model('table1')->select;
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

test 'update and id option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    id => [1, 2],
    param => {key3 => 4}
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 4);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 0, key2 => 2, key3 => 3});
$dbi->update(
    table => 'table1',
    primary_key => 'key1',
    id => 0,
    param => {key3 => 4}
);
is($dbi->select(table => 'table1')->one->{key1}, 0);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 4);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->update(
    {key3 => 4},
    table => 'table1',
    primary_key => ['key1', 'key2'],
    id => [1, 2]
);
is($dbi->select(table => 'table1')->one->{key1}, 1);
is($dbi->select(table => 'table1')->one->{key2}, 2);
is($dbi->select(table => 'table1')->one->{key3}, 4);


test 'model update and id option';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1')->update(
    id => [1, 2],
    param => {key3 => 4}
);
$result = $dbi->model('table1')->select;
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 4);


test 'delete and id option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->delete(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    id => [1, 2],
);
is_deeply($dbi->select(table => 'table1')->all, []);

$dbi->insert(table => 'table1', param => {key1 => 0, key2 => 2, key3 => 3});
$dbi->delete(
    table => 'table1',
    primary_key => 'key1',
    id => 0,
);
is_deeply($dbi->select(table => 'table1')->all, []);


test 'model delete and id option';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
eval { $dbi->execute('drop table table3') };
$dbi->execute($create_table1_2);
$dbi->execute($create_table2_2);
$dbi->execute($create_table3);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1')->delete(id => [1, 2]);
is_deeply($dbi->select(table => 'table1')->all, []);
$dbi->insert(table => 'table2', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1_1')->delete(id => [1, 2]);
is_deeply($dbi->select(table => 'table1')->all, []);
$dbi->insert(table => 'table3', param => {key1 => 1, key2 => 2, key3 => 3});
$dbi->model('table1_3')->delete(id => [1, 2]);
is_deeply($dbi->select(table => 'table1')->all, []);


test 'select and id option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    id => [1, 2]
);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 0, key2 => 2, key3 => 3});
$result = $dbi->select(
    table => 'table1',
    primary_key => 'key1',
    id => 0,
);
$row = $result->one;
is($row->{key1}, 0);
is($row->{key2}, 2);
is($row->{key3}, 3);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->select(
    table => 'table1',
    primary_key => ['key1', 'key2'],
    id => [1, 2]
);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);


test 'model select_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3});
$result = $dbi->model('table1')->select(id => [1, 2]);
$row = $result->one;
is($row->{key1}, 1);
is($row->{key2}, 2);
is($row->{key3}, 3);

test 'column separator is default .';
$dbi = MyDBI7->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->model('table1');
$result = $model->select(
    column => [$model->column('table2')],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {'table2.key1' => 1, 'table2.key3' => 3});

$result = $model->select(
    column => [$model->column('table2' => [qw/key1 key3/])],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {'table2.key1' => 1, 'table2.key3' => 3});



test 'separator';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);

$dbi->create_model(
    table => 'table1',
    join => [
       'left outer join table2 on table1.key1 = table2.key1'
    ],
    primary_key => ['key1'],
);
$model2 = $dbi->create_model(
    table => 'table2',
);
$dbi->setup_model;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->model('table1');
$result = $model->select(
    column => [
        $model->mycolumn,
        {table2 => [qw/key1 key3/]}
    ],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {key1 => 1, key2 => 2, 'table2.key1' => 1, 'table2.key3' => 3});
is_deeply($model2->select->one, {key1 => 1, key3 => 3});

$dbi->separator('__');
$model = $dbi->model('table1');
$result = $model->select(
    column => [
        $model->mycolumn,
        {table2 => [qw/key1 key3/]}
    ],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {key1 => 1, key2 => 2, 'table2__key1' => 1, 'table2__key3' => 3});
is_deeply($model2->select->one, {key1 => 1, key3 => 3});

$dbi->separator('-');
$model = $dbi->model('table1');
$result = $model->select(
    column => [
        $model->mycolumn,
        {table2 => [qw/key1 key3/]}
    ],
    where => {'table1.key1' => 1}
);
is_deeply($result->one,
          {key1 => 1, key2 => 2, 'table2-key1' => 1, 'table2-key3' => 3});
is_deeply($model2->select->one, {key1 => 1, key3 => 3});


test 'filter_off';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);

$dbi->create_model(
    table => 'table1',
    join => [
       'left outer join table2 on table1.key1 = table2.key1'
    ],
    primary_key => ['key1'],
);
$dbi->setup_model;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$model = $dbi->model('table1');
$result = $model->select(column => 'key1');
$result->filter(key1 => sub { $_[0] * 2 });
is_deeply($result->one, {key1 => 2});

test 'available_datetype';
$dbi = DBIx::Custom->connect;
ok($dbi->can('available_datatype'));


test 'select prefix option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$rows = $dbi->select(prefix => 'key1,', column => 'key2', table => 'table1')->all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "table");


test 'separator';
$dbi = DBIx::Custom->connect;
is($dbi->separator, '.');
$dbi->separator('-');
is($dbi->separator, '-');
$dbi->separator('__');
is($dbi->separator, '__');
eval { $dbi->separator('?') };
like($@, qr/Separator/);


test 'map_param';
$dbi = DBIx::Custom->connect;
$param = $dbi->map_param(
    {id => 1, author => 'Ken', price => 1900},
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', {if => sub { $_[0] eq 1900 }}]
);
is_deeply($param, {'book.id' => 1, 'book.author' => '%Ken%',
  'book.price' => 1900});

$param = $dbi->map_param(
    {id => 0, author => 0, price => 0},
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', sub { '%' . $_[0] . '%' },
      {if => sub { $_[0] eq 0 }}]
);
is_deeply($param, {'book.id' => 0, 'book.author' => '%0%', 'book.price' => '%0%'});

$param = $dbi->map_param(
    {id => '', author => '', price => ''},
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', sub { '%' . $_[0] . '%' },
      {if => sub { $_[0] eq 1 }}]
);
is_deeply($param, {});

$param = $dbi->map_param(
    {id => undef, author => undef, price => undef},
    id => 'book.id',
    price => ['book.price', {if => 'exists'}]
);
is_deeply($param, {'book.price' => undef});

$param = $dbi->map_param(
    {price => 'a'},
    id => ['book.id', {if => 'exists'}],
    price => ['book.price', sub { '%' . $_[0] }, {if => 'exists'}]
);
is_deeply($param, {'book.price' => '%a'});


test 'table_alias';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    into1 => {
        date => sub { uc $_[0] }
    }
);
$dbi->execute("insert into table1 (key1) values (:table2.key1)", {'table2.key1' => 'a'},
  table_alias => {table2 => 'table1'});
$result = $dbi->select(table => 'table1');
is($result->one->{key1}, 'A');


test 'order';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1, key2)");
$dbi->insert({key1 => 1, key2 => 1}, table => 'table1');
$dbi->insert({key1 => 1, key2 => 3}, table => 'table1');
$dbi->insert({key1 => 2, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 2, key2 => 4}, table => 'table1');
my $order = $dbi->order;
$order->prepend('key1', 'key2 desc');
$result = $dbi->select(table => 'table1', append => "$order");
is_deeply($result->all, [{key1 => 1, key2 => 3}, {key1 => 1, key2 => 1},
  {key1 => 2, key2 => 4}, {key1 => 2, key2 => 2}]);
$order->prepend('key1 desc');
$result = $dbi->select(table => 'table1', append => "$order");
is_deeply($result->all, [{key1 => 2, key2 => 4}, {key1 => 2, key2 => 2},
  {key1 => 1, key2 => 3}, {key1 => 1, key2 => 1}]);

$order = $dbi->order;
$order->prepend(['table1-key1'], [qw/table1-key2 desc/]);
$result = $dbi->select(table => 'table1',
  column => [[key1 => 'table1-key1'], [key2 => 'table1-key2']],
  append => "$order");
is_deeply($result->all, [{'table1-key1' => 1, 'table1-key2' => 3},
  {'table1-key1' => 1, 'table1-key2' => 1},
  {'table1-key1' => 2, 'table1-key2' => 4},
  {'table1-key1' => 2, 'table1-key2' => 2}]);

test 'tag_parse';
$dbi = DBIx::Custom->connect;
$dbi->tag_parse(0);
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1, key2)");
$dbi->insert({key1 => 1, key2 => 1}, table => 'table1');
eval {$dbi->execute("select * from table1 where {= key1}", {key1 => 1})};
ok($@);

test 'last_sql';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1, key2)");
$dbi->execute('select * from table1');
is($dbi->last_sql, 'select * from table1;');

eval{$dbi->execute("aaa")};
is($dbi->last_sql, 'aaa;');

test 'DBIx::Custom header';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1, key2)");
$result = $dbi->execute('select key1 as h1, key2 as h2 from table1');
is_deeply($result->header, [qw/h1 h2/]);

test 'Named placeholder :name(operater) syntax';
$dbi->execute('drop table table1');
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where :key1{=} and :key2{=}";
$result = $dbi->execute($source, param => {key1 => 1, key2 => 2});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$source = "select * from table1 where :key1{ = } and :key2{=}";
$result = $dbi->execute($source, param => {key1 => 1, key2 => 2});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$source = "select * from table1 where :key1{<} and :key2{=}";
$result = $dbi->execute($source, param => {key1 => 5, key2 => 2});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$source = "select * from table1 where :table1.key1{=} and :table1.key2{=}";
$result = $dbi->execute(
    $source,
    param => {'table1.key1' => 1, 'table1.key2' => 1},
    filter => {'table1.key2' => sub { $_[0] * 2 }}
);
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

test 'high perfomance way';
$dbi->execute('drop table table1');
$dbi->execute("create table table1 (ab, bc, ik, hi, ui, pq, dc);");
$rows = [
    {ab => 1, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 7},
    {ab => 1, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 8},
];
{
    my $query;
    foreach my $row (@$rows) {
      $query ||= $dbi->insert($row, table => 'table1', query => 1);
      $dbi->execute($query, $row, filter => {ab => sub { $_[0] * 2 }});
    }
    is_deeply($dbi->select(table => 'table1')->all,
      [
          {ab => 2, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 7},
          {ab => 2, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 8},
      ]
    );
}

$dbi->execute('drop table table1');
$dbi->execute("create table table1 (ab, bc, ik, hi, ui, pq, dc);");
$rows = [
    {ab => 1, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 7},
    {ab => 1, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 8},
];
{
    my $query;
    my $sth;
    foreach my $row (@$rows) {
      $query ||= $dbi->insert($row, table => 'table1', query => 1);
      $sth ||= $query->sth;
      $sth->execute(map { $row->{$_} } sort keys %$row);
    }
    is_deeply($dbi->select(table => 'table1')->all,
      [
          {ab => 1, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 7},
          {ab => 1, bc => 2, ik => 3, hi => 4, ui => 5, pq => 6, dc => 8},
      ]
    );
}

test 'result';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');

$result = $dbi->select(table => 'table1');
@rows = ();
while (my $row = $result->fetch) {
    push @rows, [@$row];
}
is_deeply(\@rows, [[1, 2], [3, 4]]);

$result = $dbi->select(table => 'table1');
@rows = ();
while (my $row = $result->fetch_hash) {
    push @rows, {%$row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$result = $dbi->select(table => 'table1');
$row = $result->fetch_first;
is_deeply($row, [1, 2], "row");
$row = $result->fetch;
ok(!$row, "finished");

$result = $dbi->select(table => 'table1');
$row = $result->fetch_hash_first;
is_deeply($row, {key1 => 1, key2 => 2}, "row");
$row = $result->fetch_hash;
ok(!$row, "finished");

$dbi->execute('create table table2 (key1, key2);');
$result = $dbi->select(table => 'table2');
$row = $result->fetch_hash_first;
ok(!$row, "no row fetch");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
$dbi->insert({key1 => 5, key2 => 6}, table => 'table1');
$dbi->insert({key1 => 7, key2 => 8}, table => 'table1');
$dbi->insert({key1 => 9, key2 => 10}, table => 'table1');
$result = $dbi->select(table => 'table1');
$rows = $result->fetch_multi(2);
is_deeply($rows, [[1, 2],
                  [3, 4]], "fetch_multi first");
$rows = $result->fetch_multi(2);
is_deeply($rows, [[5, 6],
                  [7, 8]], "fetch_multi secound");
$rows = $result->fetch_multi(2);
is_deeply($rows, [[9, 10]], "fetch_multi third");
$rows = $result->fetch_multi(2);
ok(!$rows);

$result = $dbi->select(table => 'table1');
eval {$result->fetch_multi};
like($@, qr/Row count must be specified/, "Not specified row count");

$result = $dbi->select(table => 'table1');
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "fetch_multi first");
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{key1 => 5, key2 => 6},
                  {key1 => 7, key2 => 8}], "fetch_multi secound");
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{key1 => 9, key2 => 10}], "fetch_multi third");
$rows = $result->fetch_hash_multi(2);
ok(!$rows);

$result = $dbi->select(table => 'table1');
eval {$result->fetch_hash_multi};
like($@, qr/Row count must be specified/, "Not specified row count");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');

test 'fetch_all';
$result = $dbi->select(table => 'table1');
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]]);

$result = $dbi->select(table => 'table1');
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$result = $dbi->select(table => 'table1');
$result->dbi->filters({three_times => sub { $_[0] * 3}});
$result->filter({key1 => 'three_times'});

$rows = $result->fetch_all;
is_deeply($rows, [[3, 2], [9, 4]], "array");

$result = $dbi->select(table => 'table1');
$result->dbi->filters({three_times => sub { $_[0] * 3}});
$result->filter({key1 => 'three_times'});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 3, key2 => 2}, {key1 => 9, key2 => 4}], "hash");

test "query_builder";
$datas = [
    # Basic tests
    {   name            => 'placeholder basic',
        source            => "a {?  k1} b {=  k2} {<> k3} {>  k4} {<  k5} {>= k6} {<= k7} {like k8}", ,
        sql_expected    => "a ? b k2 = ? k3 <> ? k4 > ? k5 < ? k6 >= ? k7 <= ? k8 like ?;",
        columns_expected   => [qw/k1 k2 k3 k4 k5 k6 k7 k8/]
    },
    {
        name            => 'placeholder in',
        source            => "{in k1 3};",
        sql_expected    => "k1 in (?, ?, ?);",
        columns_expected   => [qw/k1 k1 k1/]
    },
    
    # Table name
    {
        name            => 'placeholder with table name',
        source            => "{= a.k1} {= a.k2}",
        sql_expected    => "a.k1 = ? a.k2 = ?;",
        columns_expected  => [qw/a.k1 a.k2/]
    },
    {   
        name            => 'placeholder in with table name',
        source            => "{in a.k1 2} {in b.k2 2}",
        sql_expected    => "a.k1 in (?, ?) b.k2 in (?, ?);",
        columns_expected  => [qw/a.k1 a.k1 b.k2 b.k2/]
    },
    {
        name            => 'not contain tag',
        source            => "aaa",
        sql_expected    => "aaa;",
        columns_expected  => [],
    }
];

for (my $i = 0; $i < @$datas; $i++) {
    my $data = $datas->[$i];
    my $builder = DBIx::Custom->new->query_builder;
    my $query = $builder->build_query($data->{source});
    is($query->{sql}, $data->{sql_expected}, "$data->{name} : sql");
    is_deeply($query->columns, $data->{columns_expected}, "$data->{name} : columns");
}

$builder = DBIx::Custom->new->query_builder;
$ret_val = $builder->register_tag(
    p => sub {
        my @args = @_;
        
        my $expand    = "? $args[0] $args[1]";
        my $columns = [2];
        return [$expand, $columns];
    }
);

$query = $builder->build_query("{p a b}");
is($query->{sql}, "? a b;", "register_tag sql");
is_deeply($query->{columns}, [2], "register_tag columns");
isa_ok($ret_val, 'DBIx::Custom::QueryBuilder');

$builder = DBIx::Custom->new->query_builder;

eval{$builder->build_query('{? }')};
like($@, qr/\QColumn name must be specified in tag "{? }"/, "? not arguments");

eval{$builder->build_query("{a }")};
like($@, qr/\QTag "a" is not registered/, "tag not exist");

$builder->register_tag({
    q => 'string'
});

eval{$builder->build_query("{q}", {})};
like($@, qr/Tag "q" must be sub reference/, "tag not code ref");

$builder->register_tag({
   r => sub {} 
});

eval{$builder->build_query("{r}")};
like($@, qr/\QTag "r" must return [STRING, ARRAY_REFERENCE]/, "tag return noting");

$builder->register_tag({
   s => sub { return ["a", ""]} 
});

eval{$builder->build_query("{s}")};
like($@, qr/\QTag "s" must return [STRING, ARRAY_REFERENCE]/, "tag return not array columns");

$builder->register_tag(
    t => sub {return ["a", []]}
);


test 'General error case';
$builder = DBIx::Custom->new->query_builder;
$builder->register_tag(
    a => sub {
        return ["? ? ?", ['']];
    }
);
eval{$builder->build_query("{a}")};
like($@, qr/\QPlaceholder count/, "placeholder count is invalid");


test 'Default tag Error case';
eval{$builder->build_query("{= }")};
like($@, qr/Column name must be specified in tag "{= }"/, "basic '=' : key not exist");

eval{$builder->build_query("{in }")};
like($@, qr/Column name and count of values must be specified in tag "{in }"/, "in : key not exist");

eval{$builder->build_query("{in a}")};
like($@, qr/\QColumn name and count of values must be specified in tag "{in }"/,
     "in : key not exist");

eval{$builder->build_query("{in a r}")};
like($@, qr/\QColumn name and count of values must be specified in tag "{in }"/,
     "in : key not exist");

test 'variouse source';
$source = "a {= b} c \\{ \\} {= \\{} {= \\}} d;";
$query = $builder->build_query($source);
is($query->sql, 'a b = ? c { } { = ? } = ? d;', "basic : 1");

$source = "abc;";
$query = $builder->build_query($source);
is($query->sql, 'abc;', "basic : 2");

$source = "{= a}";
$query = $builder->build_query($source);
is($query->sql, 'a = ?;', "only tag");

$source = "000;";
$query = $builder->build_query($source);
is($query->sql, '000;', "contain 0 value");

$source = "a {= b} }";
eval{$builder->build_query($source)};
like($@, qr/unexpected "}"/, "error : 1");

$source = "a {= {}";
eval{$builder->build_query($source)};
like($@, qr/unexpected "{"/, "error : 2");



























### a little complex test
test 'type option'; # DEPRECATED!
$dbi = DBIx::Custom->connect(
    data_source => 'dbi:SQLite:dbname=:memory:',
    dbi_option => {
        $DBD::SQLite::VERSION > 1.26 ? (sqlite_unicode => 1) : (unicode => 1)
    }
);
$binary = pack("I3", 1, 2, 3);
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1(key1, key2)');
$dbi->insert(table => 'table1', param => {key1 => $binary, key2 => 'あ'}, type => [key1 => DBI::SQL_BLOB]);
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => $binary, key2 => 'あ'}, "basic");
$result = $dbi->execute('select length(key1) as key1_length from table1');
$row = $result->one;
is($row->{key1_length}, length $binary);

test 'type_rule from';
$dbi = DBIx::Custom->connect;
$dbi->type_rule(
    from1 => {
        date => sub { uc $_[0] }
    }
);
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->insert({key1 => 'a'}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->fetch_first->[0], 'A');

$result = $dbi->select(table => 'table1');
is($result->one->{key1}, 'A');


test 'type_rule into';
$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    into1 => {
        date => sub { uc $_[0] }
    }
);
$dbi->insert({key1 => 'a'}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->one->{key1}, 'A');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 date, key2 datetime)");
$dbi->type_rule(
    into1 => [
         [qw/date datetime/] => sub { uc $_[0] }
    ]
);
$dbi->insert({key1 => 'a', key2 => 'b'}, table => 'table1');
$result = $dbi->select(table => 'table1');
$row = $result->one;
is($row->{key1}, 'A');
is($row->{key2}, 'B');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->insert({key1 => 'a', key2 => 'B'}, table => 'table1');
$dbi->type_rule(
    into1 => [
        [qw/date datetime/] => sub { uc $_[0] }
    ]
);
$result = $dbi->execute(
    "select * from table1 where key1 = :key1 and key2 = :table1.key2;",
    param => {key1 => 'a', 'table1.key2' => 'b'}
);
$row = $result->one;
is($row->{key1}, 'a');
is($row->{key2}, 'B');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->insert({key1 => 'A', key2 => 'B'}, table => 'table1');
$dbi->type_rule(
    into1 => [
        [qw/date datetime/] => sub { uc $_[0] }
    ]
);
$result = $dbi->execute(
    "select * from table1 where key1 = :key1 and key2 = :table1.key2;",
    param => {key1 => 'a', 'table1.key2' => 'b'},
    table => 'table1'
);
$row = $result->one;
is($row->{key1}, 'A');
is($row->{key2}, 'B');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 date, key2 datetime)");
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->type_rule(
    from1 => {
        date => 'twice',
    },
    into1 => {
        date => 'twice',
    }
);
$dbi->insert({key1 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->fetch->[0], 8);

test 'type_rule and filter order';
$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    into1 => {
        date => sub { $_[0] . 'b' }
    },
    into2 => {
        date => sub { $_[0] . 'c' }
    },
    from1 => {
        date => sub { $_[0] . 'd' }
    },
    from2 => {
        date => sub { $_[0] . 'e' }
    }
);
$dbi->insert({key1 => '1'}, table => 'table1', filter => {key1 => sub { $_[0] . 'a' }});
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] . 'f' });
is($result->fetch_first->[0], '1abcdef');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] . 'p' }
    },
    from2 => {
        date => sub { $_[0] . 'q' }
    },
);
$dbi->insert({key1 => '1'}, table => 'table1');
$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => {
        date => sub { $_[0] . 'd' }
    },
    from2 => {
        date => sub { $_[0] . 'e' }
    }
);
$result->filter(key1 => sub { $_[0] . 'f' });
is($result->fetch_first->[0], '1def');

test 'type_rule_off';
$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
    into1 => {
        date => sub { $_[0] * 2 },
    }
);
$dbi->insert({key1 => 2}, table => 'table1', type_rule_off => 1);
$result = $dbi->select(table => 'table1', type_rule_off => 1);
is($result->type_rule_off->fetch->[0], 2);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
    into1 => {
        date => sub { $_[0] * 3 },
    }
);
$dbi->insert({key1 => 2}, table => 'table1', type_rule_off => 1);
$result = $dbi->select(table => 'table1', type_rule_off => 1);
is($result->one->{key1}, 4);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
    into1 => {
        date => sub { $_[0] * 3 },
    }
);
$dbi->insert({key1 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->one->{key1}, 12);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
    into1 => {
        date => sub { $_[0] * 3 },
    }
);
$dbi->insert({key1 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->fetch->[0], 12);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->register_filter(ppp => sub { uc $_[0] });
$dbi->type_rule(
    into1 => {
        date => 'ppp'
    }
);
$dbi->insert({key1 => 'a'}, table => 'table1');
$result = $dbi->select(table => 'table1');
is($result->one->{key1}, 'A');

eval{$dbi->type_rule(
    into1 => {
        date => 'pp'
    }
)};
like($@, qr/not registered/);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
eval {
    $dbi->type_rule(
        from1 => {
            Date => sub { $_[0] * 2 },
        }
    );
};
like($@, qr/lower/);

eval {
    $dbi->type_rule(
        into1 => {
            Date => sub { $_[0] * 2 },
        }
    );
};
like($@, qr/lower/);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
    into1 => {
        date => sub { $_[0] * 3 },
    }
);
$dbi->insert({key1 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
$result->type_rule_off;
is($result->one->{key1}, 6);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
        datetime => sub { $_[0] * 4 },
    },
);
$dbi->insert({key1 => 2, key2 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => {
        date => sub { $_[0] * 3 }
    }
);
$row = $result->one;
is($row->{key1}, 6);
is($row->{key2}, 2);

$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => {
        date => sub { $_[0] * 3 }
    }
);
$row = $result->one;
is($row->{key1}, 6);
is($row->{key2}, 2);

$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => {
        date => sub { $_[0] * 3 }
    }
);
$row = $result->one;
is($row->{key1}, 6);
is($row->{key2}, 2);
$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => [date => sub { $_[0] * 3 }]
);
$row = $result->one;
is($row->{key1}, 6);
is($row->{key2}, 2);
$dbi->register_filter(fivetimes => sub { $_[0] * 5});
$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => [date => 'fivetimes']
);
$row = $result->one;
is($row->{key1}, 10);
is($row->{key2}, 2);
$result = $dbi->select(table => 'table1');
$result->type_rule(
    from1 => [date => undef]
);
$row = $result->one;
is($row->{key1}, 2);
is($row->{key2}, 2);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
);
$dbi->insert({key1 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 3 });
is($result->one->{key1}, 12);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    from1 => {
        date => sub { $_[0] * 2 },
    },
);
$dbi->insert({key1 => 2}, table => 'table1');
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 3 });
is($result->fetch->[0], 12);

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    into1 => {
        date => sub { $_[0] . 'b' }
    },
    into2 => {
        date => sub { $_[0] . 'c' }
    },
    from1 => {
        date => sub { $_[0] . 'd' }
    },
    from2 => {
        date => sub { $_[0] . 'e' }
    }
);
$dbi->insert({key1 => '1'}, table => 'table1', type_rule_off => 1);
$result = $dbi->select(table => 'table1');
is($result->type_rule_off->fetch_first->[0], '1');
$result = $dbi->select(table => 'table1');
is($result->type_rule_on->fetch_first->[0], '1de');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    into1 => {
        date => sub { $_[0] . 'b' }
    },
    into2 => {
        date => sub { $_[0] . 'c' }
    },
    from1 => {
        date => sub { $_[0] . 'd' }
    },
    from2 => {
        date => sub { $_[0] . 'e' }
    }
);
$dbi->insert({key1 => '1'}, table => 'table1', type_rule1_off => 1);
$result = $dbi->select(table => 'table1');
is($result->type_rule1_off->fetch_first->[0], '1ce');
$result = $dbi->select(table => 'table1');
is($result->type_rule1_on->fetch_first->[0], '1cde');

$dbi = DBIx::Custom->connect;
$dbi->execute("create table table1 (key1 Date, key2 datetime)");
$dbi->type_rule(
    into1 => {
        date => sub { $_[0] . 'b' }
    },
    into2 => {
        date => sub { $_[0] . 'c' }
    },
    from1 => {
        date => sub { $_[0] . 'd' }
    },
    from2 => {
        date => sub { $_[0] . 'e' }
    }
);
$dbi->insert({key1 => '1'}, table => 'table1', type_rule2_off => 1);
$result = $dbi->select(table => 'table1');
is($result->type_rule2_off->fetch_first->[0], '1bd');
$result = $dbi->select(table => 'table1');
is($result->type_rule2_on->fetch_first->[0], '1bde');

test 'prefix';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1 char(255), key2 char(255), primary key(key1))");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4}, prefix => 'or replace');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table table1 (key1 char(255), key2 char(255), primary key(key1))");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->update(table => 'table1', param => {key2 => 4},
  where => {key1 => 1}, prefix => 'or replace');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");

test 'Model class';
use MyDBI1;
$dbi = MyDBI1->connect;
eval { $dbi->execute('drop table book') };
$dbi->execute("create table book (title, author)");
$model = $dbi->model('book');
$model->insert({title => 'a', author => 'b'});
is_deeply($model->list->all, [{title => 'a', author => 'b'}], 'basic');
$dbi->execute("create table company (name)");
$model = $dbi->model('company');
$model->insert({name => 'a'});
is_deeply($model->list->all, [{name => 'a'}], 'basic');
is($dbi->models->{'book'}, $dbi->model('book'));
is($dbi->models->{'company'}, $dbi->model('company'));

$dbi = MyDBI4->connect;
eval { $dbi->execute('drop table book') };
$dbi->execute("create table book (title, author)");
$model = $dbi->model('book');
$model->insert({title => 'a', author => 'b'});
is_deeply($model->list->all, [{title => 'a', author => 'b'}], 'basic');
$dbi->execute("create table company (name)");
$model = $dbi->model('company');
$model->insert({name => 'a'});
is_deeply($model->list->all, [{name => 'a'}], 'basic');

$dbi = MyDBI5->connect;
eval { $dbi->execute('drop table company') };
eval { $dbi->execute('drop table table1') };
$dbi->execute("create table company (name)");
$dbi->execute("create table table1 (key1)");
$model = $dbi->model('company');
$model->insert({name => 'a'});
is_deeply($model->list->all, [{name => 'a'}], 'include all model');
$dbi->insert(table => 'table1', param => {key1 => 1});
$model = $dbi->model('book');
is_deeply($model->list->all, [{key1 => 1}], 'include all model');

test 'primary_key';
use MyDBI1;
$dbi = MyDBI1->connect;
$model = $dbi->model('book');
$model->primary_key(['id', 'number']);
is_deeply($model->primary_key, ['id', 'number']);

test 'columns';
use MyDBI1;
$dbi = MyDBI1->connect;
$model = $dbi->model('book');
$model->columns(['id', 'number']);
is_deeply($model->columns, ['id', 'number']);

test 'setup_model';
use MyDBI1;
$dbi = MyDBI1->connect;
eval { $dbi->execute('drop table book') };
eval { $dbi->execute('drop table company') };
eval { $dbi->execute('drop table test') };

$dbi->execute('create table book (id)');
$dbi->execute('create table company (id, name);');
$dbi->execute('create table test (id, name, primary key (id, name));');
$dbi->setup_model;
is_deeply($dbi->model('book')->columns, ['id']);
is_deeply($dbi->model('company')->columns, ['id', 'name']);














### SQLite only test
test 'quote';
$dbi = DBIx::Custom->connect;
$dbi->quote('"');
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert(table => 'table', param => {select => 1});
$dbi->delete(table => 'table', where => {select => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [], "reserved word");

















# DEPRECATED! test
test 'filter __ expression';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table company') };
eval { $dbi->execute('drop table location') };
$dbi->execute('create table company (id, name, location_id)');
$dbi->execute('create table location (id, name)');
$dbi->apply_filter('location',
  name => {in => sub { uc $_[0] } }
);

$dbi->insert(table => 'company', param => {id => 1, name => 'a', location_id => 2});
$dbi->insert(table => 'location', param => {id => 2, name => 'b'});

$result = $dbi->select(
    table => ['company', 'location'], relation => {'company.location_id' => 'location.id'},
    column => ['location.name as location__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'company', relation => {'company.location_id' => 'location.id'},
    column => ['location.name as location__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'company', relation => {'company.location_id' => 'location.id'},
    column => ['location.name as "location.name"']
);
is($result->fetch_first->[0], 'B');

test 'reserved_word_quote';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->reserved_word_quote('"');
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->apply_filter('table', update => {out => sub { $_[0] * 3}});
$dbi->insert(table => 'table', param => {select => 1});
$dbi->update(table => 'table', where => {'table.select' => 1}, param => {update => 2});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 6}], "reserved word");
