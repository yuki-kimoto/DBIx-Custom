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
                'table1',
                {class => 'table2', name => 'table2'}
            ]
        );
    }

    package MyModel2::Base1;

    use strict;
    use warnings;

    use base 'DBIx::Custom::Model';

    package MyModel2::table1;

    use strict;
    use warnings;

    use base 'MyModel2::Base1';

    sub insert {
        my ($self, $param) = @_;
        
        return $self->SUPER::insert(param => $param);
    }

    sub list { shift->select; }

    package MyModel2::table2;

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
my $create_table1 = 'create table table1 (key1 varchar, key2 varchar);';
my $create_table1_2 = 'create table table1 (key1 varchar, key2 varchar, key3 varchar, key4 varchar, key5 varchar);';
my $create_table2 = 'create table table2 (key1 varchar, key3 varchar);';
my $create_table2_2 = "create table table2 (key1 varchar, key2 varchar, key3 varchar)";
my $create_table3 = "create table table3 (key1 varchar, key2 varchar, key3 varchar)";
my $create_table_reserved = 'create table "table" ("select" varchar, "update" varchar)';

my $q = '"';
my $p = '"';

# Variables
my $dbi;
my $param;
my $params;
my $sql;
my $result;
my $row;
my @rows;
my $rows;
my $model;
my $model2;
my $where;
my $binary;

# Prepare table
$dbi = DBIx::Custom->connect;

### a little complex test

test 'Model class';
use MyDBI1;
$dbi = MyDBI1->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$model = $dbi->model('table1');
$model->insert({key1 => 'a', key2 => 'b'});
is_deeply($model->list->all, [{key1 => 'a', key2 => 'b'}], 'basic');
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table2);
$model = $dbi->model('table2');
$model->insert({key1 => 'a'});
is_deeply($model->list->all, [{key1 => 'a', key3 => undef}], 'basic');
is($dbi->models->{'table1'}, $dbi->model('table1'));
is($dbi->models->{'table2'}, $dbi->model('table2'));

$dbi = MyDBI4->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$model = $dbi->model('table1');
$model->insert({key1 => 'a', key2 => 'b'});
is_deeply($model->list->all, [{key1 => 'a', key2 => 'b'}], 'basic');
$dbi->execute($create_table2);
$model = $dbi->model('table2');
$model->insert({key1 => 'a'});
is_deeply($model->list->all, [{key1 => 'a', key3 => undef}], 'basic');

$dbi = MyDBI5->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$model = $dbi->model('table2');
$model->insert({key1 => 'a'});
is_deeply($model->list->all, [{key1 => 'a', key3 => undef}], 'include all model');
$dbi->insert(table => 'table1', param => {key1 => 1});
$model = $dbi->model('table1');
is_deeply($model->list->all, [{key1 => 1, key2 => undef}], 'include all model');

test 'primary_key';
use MyDBI1;
$dbi = MyDBI1->connect;
$model = $dbi->model('table1');
$model->primary_key(['key1', 'key2']);
is_deeply($model->primary_key, ['key1', 'key2']);

test 'columns';
use MyDBI1;
$dbi = MyDBI1->connect;
$model = $dbi->model('table1');
$model->columns(['key1', 'key2']);
is_deeply($model->columns, ['key1', 'key2']);

test 'setup_model';
use MyDBI1;
$dbi = MyDBI1->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };

$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
is_deeply($dbi->model('table1')->columns, ['key1', 'key2']);
is_deeply($dbi->model('table2')->columns, ['key1', 'key3']);


### SQLite only test
test 'prefix';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1 (key1 varchar, key2 varchar, primary key(key1));');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4}, prefix => 'or replace');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute('create table table1 (key1 varchar, key2 varchar, primary key(key1));');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->update(table => 'table1', param => {key2 => 4},
  where => {key1 => 1}, prefix => 'or replace');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 4}], "basic");


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

test 'finish statement handle';
$dbi = DBIx::Custom->connect;
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');

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

# DEPRECATED! test
test 'filter __ expression';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table2') };
eval { $dbi->execute('drop table table3') };
$dbi->execute('create table table2 (id, name, table3_id)');
$dbi->execute('create table table3 (id, name)');
$dbi->apply_filter('table3',
  name => {in => sub { uc $_[0] } }
);

$dbi->insert(table => 'table2', param => {id => 1, name => 'a', table3_id => 2});
$dbi->insert(table => 'table3', param => {id => 2, name => 'b'});

$result = $dbi->select(
    table => ['table2', 'table3'], relation => {'table2.table3_id' => 'table3.id'},
    column => ['table3.name as table3__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'table2', relation => {'table2.table3_id' => 'table3.id'},
    column => ['table3.name as table3__name']
);
is($result->fetch_first->[0], 'B');

$result = $dbi->select(
    table => 'table2', relation => {'table2.table3_id' => 'table3.id'},
    column => ['table3.name as "table3.name"']
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
