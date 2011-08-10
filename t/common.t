use Test::More;
use strict;
use warnings;
use Encode qw/encode_utf8/;
use FindBin;
use lib "$FindBin::Bin/common";

my $dbi;

plan skip_all => $ENV{DBIX_CUSTOM_SKIP_MESSAGE} || 'common.t is always skipped'
  unless $ENV{DBIX_CUSTOM_TEST_RUN}
    && eval { $dbi = DBIx::Custom->connect; 1 };

plan 'no_plan';

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
        
        $self->include_model('MyModel8');
        
        return $self;
    }
}

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};
sub test { print "# $_[0]\n" }

# Constant
my $create_table1 = $dbi->create_table1;
my $create_table1_2 = $dbi->create_table1_2;
my $create_table1_type = $dbi->create_table1_type;
my $create_table1_highperformance = $dbi->create_table1_highperformance;
my $create_table2 = $dbi->create_table2;
my $create_table2_2 = $dbi->create_table2_2;
my $create_table3 = $dbi->create_table3;
my $create_table_reserved = $dbi->create_table_reserved;
my $q = substr($dbi->quote, 0, 1);
my $p = substr($dbi->quote, 1, 1) || $q;

# Variable
# Variables
my $builder;
my $datas;
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

# Drop table
eval { $dbi->execute('drop table table1') };

# Create table
$dbi->execute($create_table1);
$model = $dbi->create_model(table => 'table1');
$model->insert({key1 => 1, key2 => 2});
is_deeply($model->select->all, [{key1 => 1, key2 => 2}]);

test 'DBIx::Custom::Result test';
$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$source = "select key1, key2 from table1";
$query = $dbi->create_query($source);
$result = $dbi->execute($query);

@rows = ();
while (my $row = $result->fetch) {
    push @rows, [@$row];
}
is_deeply(\@rows, [[1, 2], [3, 4]], "fetch");

$result = $dbi->execute($query);
@rows = ();
while (my $row = $result->fetch_hash) {
    push @rows, {%$row};
}
is_deeply(\@rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "fetch_hash");

$result = $dbi->execute($query);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]], "fetch_all");

$result = $dbi->execute($query);
$rows = $result->fetch_hash_all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "all");

test 'Insert query return value';
$source = "insert into table1 {insert_param key1 key2}";
$query = $dbi->execute($source, {}, query => 1);
$ret_val = $dbi->execute($query, param => {key1 => 1, key2 => 2});
ok($ret_val);

test 'Direct query';
$dbi->delete_all(table => 'table1');
$insert_source = "insert into table1 {insert_param key1 key2}";
$dbi->execute($insert_source, param => {key1 => 1, key2 => 2});
$result = $dbi->execute('select * from table1;');
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

test 'Filter basic';
$dbi->delete_all(table => 'table1');
$dbi->register_filter(twice       => sub { $_[0] * 2}, 
                    three_times => sub { $_[0] * 3});

$insert_source  = "insert into table1 {insert_param key1 key2};";
$insert_query = $dbi->execute($insert_source, {}, query => 1);
$insert_query->filter({key1 => 'twice'});
$dbi->execute($insert_query, param => {key1 => 1, key2 => 2});
$result = $dbi->execute('select * from table1;');
$rows = $result->filter({key2 => 'three_times'})->all;
is_deeply($rows, [{key1 => 2, key2 => 6}], "filter fetch_filter");

test 'Filter in';
$dbi->delete_all(table => 'table1');
$insert_source  = "insert into table1 {insert_param key1 key2};";
$insert_query = $dbi->execute($insert_source, {}, query => 1);
$dbi->execute($insert_query, param => {key1 => 2, key2 => 4});
$select_source = "select * from table1 where {in table1.key1 2} and {in table1.key2 2}";
$select_query = $dbi->execute($select_source,{}, query => 1);
$select_query->filter({'table1.key1' => 'twice'});
$result = $dbi->execute($select_query, param => {'table1.key1' => [1,5], 'table1.key2' => [2,4]});
$rows = $result->all;
is_deeply($rows, [{key1 => 2, key2 => 4}], "filter");

test 'DBIx::Custom::SQLTemplate basic tag';
$dbi->execute('drop table table1');
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where key1 = :key1 and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->execute($source, {}, query => 1);
$result = $dbi->execute($query, param => {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic tag1");

$source = "select * from table1 where key1 = :key1 and {<> key2} and {< key3} and {> key4} and {>= key5};";
$query = $dbi->execute($source, {}, query => 1);
$result = $dbi->execute($query, {key1 => 1, key2 => 3, key3 => 4, key4 => 3, key5 => 5});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic tag1");

$source = "select * from table1 where {<= key1} and {like key2};";
$query = $dbi->execute($source, {}, query => 1);
$result = $dbi->execute($query, param => {key1 => 1, key2 => '%2%'});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic tag2");

test 'DIB::Custom::SQLTemplate in tag';
$dbi->execute('drop table table1');
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where {in key1 2};";
$query = $dbi->execute($source, {}, query => 1);
$result = $dbi->execute($query, param => {key1 => [9, 1]});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic");

test 'DBIx::Custom::SQLTemplate insert tag';
$dbi->delete_all(table => 'table1');
$insert_source = 'insert into table1 {insert_param key1 key2 key3 key4 key5}';
$dbi->execute($insert_source, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});

$result = $dbi->execute('select * from table1;');
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}], "basic");

test 'DBIx::Custom::SQLTemplate update tag';
$dbi->delete_all(table => 'table1');
$insert_source = "insert into table1 {insert_param key1 key2 key3 key4 key5}";
$dbi->execute($insert_source, param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->execute($insert_source, param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$update_source = 'update table1 {update_param key1 key2 key3 key4} where {= key5}';
$dbi->execute($update_source, param => {key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5});

$result = $dbi->execute('select * from table1 order by key1;');
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 1, key3 => 1, key4 => 1, key5 => 5},
                  {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10}], "basic");

test 'Named placeholder';
$dbi->execute('drop table table1');
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});

$source = "select * from table1 where key1 = :key1 and key2 = :key2";
$result = $dbi->execute($source, param => {key1 => 1, key2 => 2});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$source = "select * from table1 where key1 = \n:key1\n and key2 = :key2";
$result = $dbi->execute($source, param => {key1 => 1, key2 => 2});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$source = "select * from table1 where key1 = :key1 or key1 = :key1";
$result = $dbi->execute($source, param => {key1 => [1, 2]});
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$source = "select * from table1 where key1 = :table1.key1 and key2 = :table1.key2";
$result = $dbi->execute(
    $source,
    param => {'table1.key1' => 1, 'table1.key2' => 1},
    filter => {'table1.key2' => sub { $_[0] * 2 }}
);
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5}]);

$dbi->execute('drop table table1');
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => '2011-10-14 12:19:18', key2 => 2});
$source = "select * from table1 where key1 = '2011-10-14 12:19:18' and key2 = :key2";
$result = $dbi->execute(
    $source,
    param => {'key2' => 2},
);

$rows = $result->all;
is_deeply($rows, [{key1 => '2011-10-14 12:19:18', key2 => 2}]);

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 'a:b c:d', key2 => 2});
$source = "select * from table1 where key1 = 'a\\:b c\\:d' and key2 = :key2";
$result = $dbi->execute(
    $source,
    param => {'key2' => 2},
);
$rows = $result->all;
is_deeply($rows, [{key1 => 'a:b c:d', key2 => 2}]);

test 'Error case';
eval {DBIx::Custom->connect(dsn => 'dbi:SQLit')};
ok($@, "connect error");

eval{$dbi->execute("{p }", {}, query => 1)};
ok($@, "create_query invalid SQL template");

test 'insert';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "basic");

$dbi->execute('delete from table1');
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_bind_filter('twice');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, filter => {key1 => 'three_times'});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "filter");
$dbi->default_bind_filter(undef);

$dbi->execute('drop table table1');
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, append => '   ');
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 1, key2 => 2}], 'insert append');

eval{$dbi->insert(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "invalid");

eval{$dbi->insert(table => 'table', param => {';' => 1})};
like($@, qr/safety/);

eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert(table => 'table', param => {select => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => undef}], "reserved word");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 2}, table => 'table1');
$dbi->insert({key1 => 3, key2 => 4}, table => 'table1');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "basic");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => \"'1'", key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "basic");

test 'update';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1});
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");
                  
$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => 12}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "update key same as search key");

$dbi->update(table => 'table1', param => {key2 => [12]}, where => {key2 => 2, key3 => 3});
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 12, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "update key same as search key : param is array ref");

$dbi->execute("delete from table1");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1},
              filter => {key2 => sub { $_[0] * 2 }});
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 22, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "filter");

$result = $dbi->update(table => 'table1', param => {key2 => 11}, where => {key1 => 1}, append => '   ');

eval{$dbi->update(table => 'table1', where => {key1 => 1}, noexist => 1)};
like($@, qr/noexist/, "invalid");

eval{$dbi->update(table => 'table1')};
like($@, qr/where/, "not contain where");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$where = $dbi->where;
$where->clause(['and', 'key1 = :key1', 'key2 = :key2']);
$where->param({key1 => 1, key2 => 2});
$dbi->update(table => 'table1', param => {key1 => 3}, where => $where);
$result = $dbi->select(table => 'table1');
is_deeply($result->all, [{key1 => 3, key2 => 2}], 'update() where');

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->update(
    table => 'table1',
    param => {key1 => 3},
    where => [
        ['and', 'key1 = :key1', 'key2 = :key2'],
        {key1 => 1, key2 => 2}
    ]
);
$result = $dbi->select(table => 'table1');
is_deeply($result->all, [{key1 => 3, key2 => 2}], 'update() where');

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$where = $dbi->where;
$where->clause(['and', 'key2 = :key2']);
$where->param({key2 => 2});
$dbi->update(table => 'table1', param => {key1 => 3}, where => $where);
$result = $dbi->select(table => 'table1');
is_deeply($result->all, [{key1 => 3, key2 => 2}], 'update() where');

eval{$dbi->update(table => 'table1', param => {';' => 1})};
like($@, qr/safety/);

eval{$dbi->update(table => 'table1', param => {'key1' => 1}, where => {';' => 1})};
like($@, qr/safety/);

eval { $dbi->execute('drop table table1') };
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->apply_filter('table', update => {out => sub { $_[0] * 3}});
$dbi->insert(table => 'table', param => {select => 1});
$dbi->update(table => 'table', where => {select => 1}, param => {update => 2});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 6}], "reserved word");

eval {$dbi->update_all(table => 'table', param => {';' => 2}) };
like($@, qr/safety/);

eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->apply_filter('table', update => {out => sub { $_[0] * 3}});
$dbi->insert(table => 'table', param => {select => 1});
$dbi->update(table => 'table', where => {'table.select' => 1}, param => {update => 2});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 6}], "reserved word");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update({key2 => 11}, table => 'table1', where => {key1 => 1});
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->update(table => 'table1', param => {key2 => \"'11'"}, where => {key1 => 1});
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");

test 'update_all';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1_2);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2, key3 => 3, key4 => 4, key5 => 5});
$dbi->insert(table => 'table1', param => {key1 => 6, key2 => 7, key3 => 8, key4 => 9, key5 => 10});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update_all(table => 'table1', param => {key2 => 10}, filter => {key2 => 'twice'});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 20, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 20, key3 => 8, key4 => 9, key5 => 10}],
                  "filter");


test 'delete';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "basic");

$dbi->execute("delete from table1;");
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->delete(table => 'table1', where => {key2 => 1}, filter => {key2 => 'twice'});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "filter");

$dbi->delete(table => 'table1', where => {key1 => 1}, append => '   ');

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(table => 'table1', where => {key1 => 1, key2 => 2});
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "delete multi key");

eval{$dbi->delete(table => 'table1', where => {key1 => 1}, noexist => 1)};
like($@, qr/noexist/, "invalid");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$where = $dbi->where;
$where->clause(['and', 'key1 = :key1', 'key2 = :key2']);
$where->param({ke1 => 1, key2 => 2});
$dbi->delete(table => 'table1', where => $where);
$result = $dbi->select(table => 'table1');
is_deeply($result->all, [{key1 => 3, key2 => 4}], 'delete() where');

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete(
    table => 'table1',
    where => [
        ['and', 'key1 = :key1', 'key2 = :key2'],
        {ke1 => 1, key2 => 2}
    ]
);
$result = $dbi->select(table => 'table1');
is_deeply($result->all, [{key1 => 3, key2 => 4}], 'delete() where');

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->delete(table => 'table1', where => {key1 => 1}, prefix => '    ');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [], "basic");

test 'delete error';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
eval{$dbi->delete(table => 'table1')};
like($@, qr/"where" must be specified/,
         "where key-value pairs not specified");

eval{$dbi->delete(table => 'table1', where => {';' => 1})};
like($@, qr/safety/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert(table => 'table', param => {select => 1});
$dbi->delete(table => 'table', where => {select => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [], "reserved word");

test 'delete_all';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->delete_all(table => 'table1');
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [], "basic");


test 'select';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$rows = $dbi->select(table => 'table1')->all;
is_deeply($rows, [{key1 => 1, key2 => 2},
                  {key1 => 3, key2 => 4}], "table");

$rows = $dbi->select(table => 'table1', column => ['key1'])->all;
is_deeply($rows, [{key1 => 1}, {key1 => 3}], "table and columns and where key");

$rows = $dbi->select(table => 'table1', where => {key1 => 1})->all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "table and columns and where key");

$rows = $dbi->select(table => 'table1', column => ['key1'], where => {key1 => 3})->all;
is_deeply($rows, [{key1 => 3}], "table and columns and where key");

$rows = $dbi->select(table => 'table1', append => "order by key1 desc limit 1")->all;
is_deeply($rows, [{key1 => 3, key2 => 4}], "append statement");

$dbi->register_filter(decrement => sub { $_[0] - 1 });
$rows = $dbi->select(table => 'table1', where => {key1 => 2}, filter => {key1 => 'decrement'})
            ->all;
is_deeply($rows, [{key1 => 1, key2 => 2}], "filter");

eval { $dbi->execute("drop table table2") };
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$rows = $dbi->select(
    table => [qw/table1 table2/],
    column => 'table1.key1 as table1_key1, table2.key1 as table2_key1, key2, key3',
    where   => {'table1.key2' => 2},
    relation  => {'table1.key1' => 'table2.key1'}
)->all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "relation : exists where");

$rows = $dbi->select(
    table => [qw/table1 table2/],
    column => ['table1.key1 as table1_key1', 'table2.key1 as table2_key1', 'key2', 'key3'],
    relation  => {'table1.key1' => 'table2.key1'}
)->all;
is_deeply($rows, [{table1_key1 => 1, table2_key1 => 1, key2 => 2, key3 => 5}], "relation : no exists where");

eval{$dbi->select(table => 'table1', noexist => 1)};
like($@, qr/noexist/, "invalid");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert(table => 'table', param => {select => 1, update => 2});
$result = $dbi->select(table => 'table', where => {select => 1});
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 2}], "reserved word");

test 'fetch filter';
eval { $dbi->execute('drop table table1') };
$dbi->register_filter(
    twice       => sub { $_[0] * 2 },
    three_times => sub { $_[0] * 3 }
);
$dbi->default_fetch_filter('twice');
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter({key1 => 'three_times'});
$row = $result->one;
is_deeply($row, {key1 => 3, key2 => 4}, "default_fetch_filter and filter");

test 'filters';
$dbi = DBIx::Custom->new;

is($dbi->filters->{decode_utf8}->(encode_utf8('あ')),
   'あ', "decode_utf8");

is($dbi->filters->{encode_utf8}->('あ'),
   encode_utf8('あ'), "encode_utf8");

test 'transaction1';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->dbh->begin_work;
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
$dbi->dbh->commit;
$result = $dbi->select(table => 'table1');
is_deeply(scalar $result->all, [{key1 => 1, key2 => 2}, {key1 => 2, key2 => 3}],
          "commit");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->dbh->begin_work(0);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->dbh->rollback;

$result = $dbi->select(table => 'table1');
ok(! $result->fetch_first, "rollback");

test 'execute';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
{
    local $Carp::Verbose = 0;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/\Qselect * frm table1;/, "fail prepare");
    like($@, qr/\.t /, "fail : not verbose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/Custom.*\.t /s, "fail : verbose");
}

eval{$dbi->execute('select * from table1', no_exists => 1)};
like($@, qr/wrong/, "invald SQL");

$query = $dbi->execute('select * from table1 where key1 = :key1', {}, query => 1);
$dbi->dbh->disconnect;
eval{$dbi->execute($query, param => {key1 => {a => 1}})};
ok($@, "execute fail");

{
    local $Carp::Verbose = 0;
    eval{$dbi->execute('select * from table1 where {0 key1}', {}, query => 1)};
    like($@, qr/\Q.t /, "caller spec : not vebose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->execute('select * from table1 where {0 key1}', {}, query => 1)};
    like($@, qr/QueryBuilder.*\.t /s, "caller spec : not vebose");
}


test 'transaction2';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);

$dbi->begin_work;

eval {
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    die "Error";
    $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
};

$dbi->rollback if $@;

$result = $dbi->select(table => 'table1');
$rows = $result->all;
is_deeply($rows, [], "rollback");

$dbi->begin_work;

eval {
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    $dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
};

$dbi->commit unless $@;

$result = $dbi->select(table => 'table1');
$rows = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], "commit");

$dbi->dbh->{AutoCommit} = 0;
eval{ $dbi->begin_work };
ok($@, "exception");
$dbi->dbh->{AutoCommit} = 1;

test 'cache';
eval { $dbi->execute('drop table table1') };
$dbi->cache(1);
$dbi->execute($create_table1);
$source = 'select * from table1 where key1 = :key1 and key2 = :key2;';
$dbi->execute($source, {}, query => 1);
is_deeply($dbi->{_cached}->{$source}, 
          {sql => "select * from table1 where key1 = ? and key2 = ?;", columns => ['key1', 'key2'], tables => []}, "cache");

eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->{_cached} = {};
$dbi->cache(0);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is(scalar keys %{$dbi->{_cached}}, 0, 'not cache');

test 'execute';
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
{
    local $Carp::Verbose = 0;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/\Qselect * frm table1;/, "fail prepare");
    like($@, qr/\.t /, "fail : not verbose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->execute('select * frm table1')};
    like($@, qr/Custom.*\.t /s, "fail : verbose");
}

eval{$dbi->execute('select * from table1', no_exists => 1)};
like($@, qr/wrong/, "invald SQL");

$query = $dbi->execute('select * from table1 where key1 = :key1', {}, query => 1);
$dbi->dbh->disconnect;
eval{$dbi->execute($query, param => {key1 => {a => 1}})};
ok($@, "execute fail");

{
    local $Carp::Verbose = 0;
    eval{$dbi->execute('select * from table1 where {0 key1}', {}, query => 1)};
    like($@, qr/\Q.t /, "caller spec : not vebose");
}
{
    local $Carp::Verbose = 1;
    eval{$dbi->execute('select * from table1 where {0 key1}', {}, query => 1)};
    like($@, qr/QueryBuilder.*\.t /s, "caller spec : not vebose");
}

test 'method';
$dbi->method(
    one => sub { 1 }
);
$dbi->method(
    two => sub { 2 }
);
$dbi->method({
    twice => sub {
        my $self = shift;
        return $_[0] * 2;
    }
});

is($dbi->one, 1, "first");
is($dbi->two, 2, "second");
is($dbi->twice(5), 10 , "second");

eval {$dbi->XXXXXX};
ok($@, "not exists");

test 'out filter';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3});
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'three_times'}, 
              'key2' => {out => 'three_times', in => 'twice'});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->execute('select * from table1;');
$row   = $result->fetch_hash_first;
is_deeply($row, {key1 => 2, key2 => 6}, "insert");
$result = $dbi->select(table => 'table1');
$row   = $result->one;
is_deeply($row, {key1 => 6, key2 => 12}, "insert");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3});
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'three_times'}, 
              'key2' => {out => 'three_times', in => 'twice'});
$dbi->apply_filter(
    'table1', 'key1' => {out => undef}
); 
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->execute('select * from table1;');
$row   = $result->one;
is_deeply($row, {key1 => 1, key2 => 6}, "insert");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, filter => {key1 => undef});
$dbi->update(table => 'table1', param => {key1 => 2}, where => {key2 => 2});
$result = $dbi->execute('select * from table1;');
$row   = $result->one;
is_deeply($row, {key1 => 4, key2 => 2}, "update");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1=> undef});
$dbi->delete(table => 'table1', where => {key1 => 1});
$result = $dbi->execute('select * from table1;');
$rows   = $result->all;
is_deeply($rows, [], "delete");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1 => undef});
$result = $dbi->select(table => 'table1', where => {key1 => 1});
$result->filter({'key2' => 'twice'});
$rows   = $result->all;
is_deeply($rows, [{key1 => 4, key2 => 4}], "select");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1 => undef});
$result = $dbi->execute("select * from table1 where key1 = :key1 and key2 = :key2;",
                        param => {key1 => 1, key2 => 2},
                        table => ['table1']);
$rows   = $result->all;
is_deeply($rows, [{key1 => 4, key2 => 2}], "execute");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
    'table1', 'key1' => {out => 'twice', in => 'twice'}
);
$dbi->insert(table => 'table1', param => {key1 => 2, key2 => 2}, filter => {key1 => undef});
$result = $dbi->execute("select * from {table table1} where key1 = :key1 and key2 = :key2;",
                        param => {key1 => 1, key2 => 2});
$rows   = $result->all;
is_deeply($rows, [{key1 => 4, key2 => 2}], "execute table tag");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3 });
$dbi->apply_filter(
    'table1', 'key2' => {out => 'twice', in => 'twice'}
);
$dbi->apply_filter(
    'table2', 'key3' => {out => 'three_times', in => 'three_times'}
);
$dbi->insert(table => 'table1', param => {key1 => 5, key2 => 2}, filter => {key2 => undef});
$dbi->insert(table => 'table2', param => {key1 => 5, key3 => 6}, filter => {key3 => undef});
$result = $dbi->select(
     table => ['table1', 'table2'],
     column => ['key2', 'key3'],
     where => {'table1.key2' => 1, 'table2.key3' => 2}, relation => {'table1.key1' => 'table2.key1'});

$result->filter({'key2' => 'twice'});
$rows   = $result->all;
is_deeply($rows, [{key2 => 4, key3 => 18}], "select : join");

$result = $dbi->select(
     table => ['table1', 'table2'],
     column => ['key2', 'key3'],
     where => {'key2' => 1, 'key3' => 2}, relation => {'table1.key1' => 'table2.key1'});

$result->filter({'key2' => 'twice'});
$rows   = $result->all;
is_deeply($rows, [{key2 => 4, key3 => 18}], "select : join : omit");

test 'each_column';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
eval { $dbi->execute('drop table table3') };
$dbi->execute($create_table1_type);
$dbi->execute($create_table2);

$infos = [];
$dbi->each_column(sub {
    my ($self, $table, $column, $cinfo) = @_;
    
    if ($table =~ /^table\d/) {
         my $info = [$table, $column, $cinfo->{COLUMN_NAME}];
         push @$infos, $info;
    }
});
$infos = [sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @$infos];
is_deeply($infos, 
    [
        ['table1', 'key1', 'key1'],
        ['table1', 'key2', 'key2'],
        ['table2', 'key1', 'key1'],
        ['table2', 'key3', 'key3']
    ]
    
);
test 'each_table';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table2);
$dbi->execute($create_table1_type);

$infos = [];
$dbi->each_table(sub {
    my ($self, $table, $table_info) = @_;
    
    if ($table =~ /^table\d/) {
         my $info = [$table, $table_info->{TABLE_NAME}];
         push @$infos, $info;
    }
});
$infos = [sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @$infos];
is_deeply($infos, 
    [
        ['table1', 'table1'],
        ['table2', 'table2'],
    ]
);

test 'limit';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 4});
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 6});
$dbi->register_tag(
    limit => sub {
        my ($count, $offset) = @_;
        
        my $s = '';
        $s .= "limit $count";
        $s .= " offset $offset" if defined $offset;
        
        return [$s, []];
    }
);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 1 0}"
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 2 1}"
)->all;
is_deeply($rows, [{key1 => 1, key2 => 4},{key1 => 1, key2 => 6}]);
$rows = $dbi->select(
  table => 'table1',
  where => {key1 => 1},
  append => "order by key2 {limit 1}"
)->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

test 'connect super';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->one->{key1}, 1);

$dbi = DBIx::Custom->new;
$dbi->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->one->{key1}, 1);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is($dbi->select(table => 'table1')->one->{key1}, 1);

test 'end_filter';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->end_filter(key1 => sub { $_[0] * 3 }, key2 => sub { $_[0] * 5 });
$row = $result->fetch_first;
is_deeply($row, [6, 40]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter([qw/key1 key2/] => sub { $_[0] * 2 });
$result->end_filter([[qw/key1 key2/] => sub { $_[0] * 3 }]);
$row = $result->fetch_first;
is_deeply($row, [6, 12]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$result->filter([[qw/key1 key2/] => sub { $_[0] * 2 }]);
$result->end_filter([qw/key1 key2/] => sub { $_[0] * 3 });
$row = $result->fetch_first;
is_deeply($row, [6, 12]);

$dbi->register_filter(five_times => sub { $_[0] * 5 });
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->end_filter({key1 => sub { $_[0] * 3 }, key2 => 'five_times' });
$row = $result->one;
is_deeply($row, {key1 => 6, key2 => 40});

$dbi->register_filter(five_times => sub { $_[0] * 5 });
$dbi->apply_filter('table1',
    key1 => {end => sub { $_[0] * 3 } },
    key2 => {end => 'five_times'}
);
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$row = $result->one;
is_deeply($row, {key1 => 6, key2 => 40}, 'apply_filter');

$dbi->register_filter(five_times => sub { $_[0] * 5 });
$dbi->apply_filter('table1',
    key1 => {end => sub { $_[0] * 3 } },
    key2 => {end => 'five_times'}
);
$result = $dbi->select(table => 'table1');
$result->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 });
$result->filter(key1 => undef);
$result->end_filter(key1 => undef);
$row = $result->one;
is_deeply($row, {key1 => 1, key2 => 40}, 'apply_filter overwrite');

test 'remove_end_filter and remove_filter';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1');
$row = $result
       ->filter(key1 => sub { $_[0] * 2 }, key2 => sub { $_[0] * 4 })
       ->remove_filter
       ->end_filter(key1 => sub { $_[0] * 3 }, key2 => sub { $_[0] * 5 })
       ->remove_end_filter
       ->fetch_first;
is_deeply($row, [1, 2]);

test 'empty where select';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$result = $dbi->select(table => 'table1', where => {});
$row = $result->one;
is_deeply($row, {key1 => 1, key2 => 2});

test 'select query option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$query = $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2}, query => 1);
is(ref $query, 'DBIx::Custom::Query');
$query = $dbi->update(table => 'table1', where => {key1 => 1}, param => {key2 => 2}, query => 1);
is(ref $query, 'DBIx::Custom::Query');
$query = $dbi->delete(table => 'table1', where => {key1 => 1}, query => 1);
is(ref $query, 'DBIx::Custom::Query');
$query = $dbi->select(table => 'table1', where => {key1 => 1, key2 => 2}, query => 1);
is(ref $query, 'DBIx::Custom::Query');

test 'where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$where = $dbi->where->clause(['and', 'key1 = :key1', 'key2 = :key2']);
is("$where", "where ( key1 = :key1 and key2 = :key2 )", 'no param');

$where = $dbi->where
             ->clause(['and', 'key1 = :key1', 'key2 = :key2'])
             ->param({key1 => 1});

$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$result = $dbi->select(
    table => 'table1',
    where => [
        ['and', 'key1 = :key1', 'key2 = :key2'],
        {key1 => 1}
    ]
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(['and', 'key1 = :key1', 'key2 = :key2'])
             ->param({key1 => 1, key2 => 2});
$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(['and', 'key1 = :key1', 'key2 = :key2'])
             ->param({});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where
             ->clause(['and', ['or', 'key1 > :key1', 'key1 < :key1'], 'key2 = :key2'])
             ->param({key1 => [0, 3], key2 => 2});
$result = $dbi->select(
    table => 'table1',
    where => $where,
); 
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where;
$result = $dbi->select(
    table => 'table1',
    where => $where
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

eval {
$where = $dbi->where
             ->clause(['uuu']);
$result = $dbi->select(
    table => 'table1',
    where => $where
);
};
ok($@);

$where = $dbi->where;
is("$where", '');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 2])
             ->param({key1 => [1, 3]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 2])
             ->param({key1 => [1]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 2])
             ->param({key1 => 1});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause('key1 = :key1')
             ->param({key1 => 1});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where
             ->clause('key1 = :key1 key2 = :key2')
             ->param({key1 => 1});
eval{$where->to_string};
like($@, qr/one column/);

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [$dbi->not_exists, 1, 3]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [1, $dbi->not_exists, 3]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [1, 3, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [1, $dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [$dbi->not_exists, 1, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [$dbi->not_exists, $dbi->not_exists, 1]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => [$dbi->not_exists, $dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['or', ('key1 = :key1') x 3])
             ->param({key1 => []});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [2, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [$dbi->not_exists, 2]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [$dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2},{key1 => 3, key2 => 4}], 'not_exists');

$where = $dbi->where
             ->clause(['and', '{> key1}', '{< key1}' ])
             ->param({key1 => [0, 2]});
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}], 'not_exists');

$where = $dbi->where
             ->clause(['and', 'key1 is not null', 'key2 is not null' ]);
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}], 'not_exists');

eval {$dbi->where(ppp => 1) };
like($@, qr/invalid/);

$where = $dbi->where(
    clause => ['and', ['or'], ['and', 'key1 = :key1', 'key2 = :key2']],
    param => {key1 => 1, key2 => 2}
);
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);


$where = $dbi->where(
    clause => ['and', ['or'], ['or', ':key1', ':key2']],
    param => {}
);
$result = $dbi->select(
    table => 'table1',
    where => $where,
);
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => undef});
$result = $dbi->execute("select * from table1 $where", {key1 => 1});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => undef});
$where->if('defined');
$where->map;
$result = $dbi->execute("select * from table1 $where", {key1 => 1});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where;
$where->clause(['or', ':key1{=}', ':key1{=}']);
$where->param({key1 => [undef, undef]});
$result = $dbi->execute("select * from table1 $where", {key1 => [1, 0]});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);
$result = $dbi->execute("select * from table1 $where", {key1 => [0, 1]});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => [undef, undef]});
$where->if('defined');
$where->map;
$result = $dbi->execute("select * from table1 $where", {key1 => [1, 0]});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);
$result = $dbi->execute("select * from table1 $where", {key1 => [0, 1]});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => 0});
$where->if('length');
$where->map;
$result = $dbi->execute("select * from table1 $where", {key1 => 1});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => ''});
$where->if('length');
$where->map;
$result = $dbi->execute("select * from table1 $where", {key1 => 1});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => 5});
$where->if(sub { ($_[0] || '') eq 5 });
$where->map;
$result = $dbi->execute("select * from table1 $where", {key1 => 1});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}]);

$where = $dbi->where;
$where->clause(['and', ':key1{=}']);
$where->param({key1 => 7});
$where->if(sub { ($_[0] || '') eq 5 });
$where->map;
$result = $dbi->execute("select * from table1 $where", {key1 => 1});
$row = $result->all;
is_deeply($row, [{key1 => 1, key2 => 2}, {key1 => 3, key2 => 4}]);

$where = $dbi->where;
$where->param({id => 1, author => 'Ken', price => 1900});
$where->map(id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', {if => sub { $_[0] eq 1900 }}]
);
is_deeply($where->param, {'book.id' => 1, 'book.author' => '%Ken%',
  'book.price' => 1900});

$where = $dbi->where;
$where->param({id => 0, author => 0, price => 0});
$where->map(
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', sub { '%' . $_[0] . '%' },
      {if => sub { $_[0] eq 0 }}]
);
is_deeply($where->param, {'book.id' => 0, 'book.author' => '%0%', 'book.price' => '%0%'});

$where = $dbi->where;
$where->param({id => '', author => '', price => ''});
$where->if('length');
$where->map(
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', sub { '%' . $_[0] . '%' },
      {if => sub { $_[0] eq 1 }}]
);
is_deeply($where->param, {});

$where = $dbi->where;
$where->param({id => undef, author => undef, price => undef});
$where->if('length');
$where->map(
    id => 'book.id',
    price => ['book.price', {if => 'exists'}]
);
is_deeply($where->param, {'book.price' => undef});

$where = $dbi->where;
$where->param({price => 'a'});
$where->if('length');
$where->map(
    id => ['book.id', {if => 'exists'}],
    price => ['book.price', sub { '%' . $_[0] }, {if => 'exists'}]
);
is_deeply($where->param, {'book.price' => '%a'});

$where = $dbi->where;
$where->param({id => [1, 2], author => 'Ken', price => 1900});
$where->map(
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', {if => sub { $_[0] eq 1900 }}]
);
is_deeply($where->param, {'book.id' => [1, 2], 'book.author' => '%Ken%',
  'book.price' => 1900});

$where = $dbi->where;
$where->if('length');
$where->param({id => ['', ''], author => 'Ken', price => 1900});
$where->map(
    id => 'book.id',
    author => ['book.author', sub { '%' . $_[0] . '%' }],
    price => ['book.price', {if => sub { $_[0] eq 1900 }}]
);
is_deeply($where->param, {'book.id' => [$dbi->not_exists, $dbi->not_exists], 'book.author' => '%Ken%',
  'book.price' => 1900});

$where = $dbi->where;
$where->param({id => ['', ''], author => 'Ken', price => 1900});
$where->map(
    id => ['book.id', {if => 'length'}],
    author => ['book.author', sub { '%' . $_[0] . '%' }, {if => 'defined'}],
    price => ['book.price', {if => sub { $_[0] eq 1900 }}]
);
is_deeply($where->param, {'book.id' => [$dbi->not_exists, $dbi->not_exists], 'book.author' => '%Ken%',
  'book.price' => 1900});

test 'dbi_option default';
$dbi = DBIx::Custom->new;
is_deeply($dbi->dbi_option, {});

test 'register_tag_processor';
$dbi = DBIx::Custom->connect;
$dbi->register_tag_processor(
    a => sub { 1 }
);
is($dbi->query_builder->tag_processors->{a}->(), 1);

test 'register_tag';
$dbi = DBIx::Custom->connect;
$dbi->register_tag(
    b => sub { 2 }
);
is($dbi->query_builder->tags->{b}->(), 2);

test 'table not specify exception';
$dbi = DBIx::Custom->connect;
eval {$dbi->insert};
like($@, qr/table/);
eval {$dbi->update};
like($@, qr/table/);
eval {$dbi->delete};
like($@, qr/table/);
eval {$dbi->select};
like($@, qr/table/);

test 'more tests';
$dbi = DBIx::Custom->connect;
eval{$dbi->apply_filter('table', 'column', [])};
like($@, qr/apply_filter/);

eval{$dbi->apply_filter('table', 'column', {outer => 2})};
like($@, qr/apply_filter/);

$dbi->apply_filter(

);
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->apply_filter('table1', 'key2', 
                   {in => sub { $_[0] * 3 }, out => sub { $_[0] * 2 }});
$rows = $dbi->select(table => 'table1', where => {key2 => 1})->all;
is_deeply($rows, [{key1 => 1, key2 => 6}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
$dbi->apply_filter('table1', 'key2', {});
$rows = $dbi->select(table => 'table1', where => {key2 => 2})->all;
is_deeply($rows, [{key1 => 1, key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval {$dbi->apply_filter('table1', 'key2', {out => 'no'})};
like($@, qr/not registered/);
eval {$dbi->apply_filter('table1', 'key2', {in => 'no'})};
like($@, qr/not registered/);
$dbi->method({one => sub { 1 }});
is($dbi->one, 1);

eval{DBIx::Custom->connect(dsn => undef)};
like($@, qr/_connect/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2},
             filter => {key1 => 'twice'});
$row = $dbi->select(table => 'table1')->one;
is_deeply($row, {key1 => 2, key2 => 2});
eval {$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2},
             filter => {key1 => 'no'}) };
like($@, qr//);

$dbi->register_filter(one => sub { });
$dbi->default_fetch_filter('one');
ok($dbi->default_fetch_filter);
$dbi->default_bind_filter('one');
ok($dbi->default_bind_filter);
eval{$dbi->default_fetch_filter('no')};
like($@, qr/not registered/);
eval{$dbi->default_bind_filter('no')};
like($@, qr/not registered/);
$dbi->default_bind_filter(undef);
ok(!defined $dbi->default_bind_filter);
$dbi->default_fetch_filter(undef);
ok(!defined $dbi->default_fetch_filter);
eval {$dbi->execute('select * from table1 {} {= author') };
like($@, qr/Tag not finished/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->register_filter(one => sub { 1 });
$result = $dbi->select(table => 'table1');
eval {$result->filter(key1 => 'no')};
like($@, qr/not registered/);
eval {$result->end_filter(key1 => 'no')};
like($@, qr/not registered/);
$result->default_filter(undef);
ok(!defined $result->default_filter);
$result->default_filter('one');
is($result->default_filter->(), 1);

test 'dbi_option';
$dbi = DBIx::Custom->connect(dbi_option => {PrintError => 1});
ok($dbi->dbh->{PrintError});
$dbi = DBIx::Custom->connect(dbi_options => {PrintError => 1});
ok($dbi->dbh->{PrintError});

test 'DBIx::Custom::Result stash()';
$result = DBIx::Custom::Result->new;
is_deeply($result->stash, {}, 'default');
$result->stash->{foo} = 1;
is($result->stash->{foo}, 1, 'get and set');

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
eval { $dbi->execute('drop table table2') };
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

test 'order';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
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
$dbi->execute($create_table1);
$dbi->insert({key1 => 1, key2 => 1}, table => 'table1');
eval {$dbi->execute("select * from table1 where {= key1}", {key1 => 1})};
ok($@);

test 'last_sql';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->execute('select * from table1');
is($dbi->last_sql, 'select * from table1;');

eval{$dbi->execute("aaa")};
is($dbi->last_sql, 'aaa;');

test 'DBIx::Custom header';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
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
$dbi->execute($create_table1_highperformance);
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
$dbi->execute($create_table1_highperformance);
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

test 'dbi method from model';
$dbi = MyDBI9->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->setup_model;
$model = $dbi->model('table1');
eval{$model->execute('select * from table1')};
ok(!$@);

test 'column table option';
$dbi = MyDBI9->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
eval { $dbi->execute('drop table table2') };
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
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 3});
$model = $dbi->create_model(
    table => 'table2'
);
$model->method(foo => sub { shift->select(@_) });
is_deeply($model->foo->one, {key1 => 1, key3 => 3});

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
$result = $dbi->execute('select * from table1 order by key1;', table => 'table1');
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
$result = $dbi->execute('select * from table1 order by key1;', table => 'table1');
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
$result = $dbi->execute('select * from table1 order by key1;', table => 'table1');
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
$result = $dbi->execute('select * from table1 order by key1;');
$rows   = $result->all;
is_deeply($rows, [{key1 => 1, key2 => 11, key3 => 3, key4 => 4, key5 => 5},
                  {key1 => 6, key2 => 7,  key3 => 8, key4 => 9, key5 => 10}],
                  "basic");

test 'join';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
$dbi->insert(table => 'table1', param => {key1 => 3, key2 => 4});
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
eval { $dbi->execute('drop table table3') };
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
eval { $dbi->execute('drop table table1') };
$dbi->execute($create_table1);
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
eval { $dbi->execute('drop table table2') };
$dbi->execute($create_table2);
$dbi->insert(table => 'table2', param => {key1 => 1, key3 => 5});
$rows = $dbi->select(
    table => 'table1',
    column => "${q}table1$p.${q}key1$p as ${q}table1_key1$p, ${q}table2$p.${q}key1$p as ${q}table2_key1$p, ${q}key2$p, ${q}key3$p",
    where   => {'table1.key2' => 2},
    join  => ["left outer join ${q}table2$p on ${q}table1$p.${q}key1$p = ${q}table2$p.${q}key1$p"],
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




1;
