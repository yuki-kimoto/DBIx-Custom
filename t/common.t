use Test::More;
use strict;
use warnings;
use Encode qw/encode_utf8/;
use FindBin;
use Scalar::Util 'isweak';

$ENV{DBIX_CUSTOM_SUPPRESS_DEPRECATION} = '0.28';

my $dbi;

plan skip_all => $ENV{DBIX_CUSTOM_SKIP_MESSAGE} || 'common.t is always skipped'
  unless $ENV{DBIX_CUSTOM_TEST_RUN}
    && eval { $dbi = DBIx::Custom->connect; 1 };

plan 'no_plan';

sub test { print "# $_[0]\n" }

# Dot to under score
sub u($) {
  my $value = shift;
  $value =~ s/\./_/g;
  return $value;
}

sub u2($) {
  my $value = shift;
  $value =~ s/\./__/g;
  return $value;
}

sub hy($) {
  my $value = shift;
  $value =~ s/\./-/g;
  return $value;
}

sub colon2 {
  my $value = shift;
  $value =~ s/\./::/g;
  return $value;
}

sub table_only {
  my $value = shift;
  $value =~ s/^.+\.//;
  return $value;
}

# Constant
my $table1 = $dbi->table1;
my $table2 = $dbi->table2;
my $table2_alias = $dbi->table2_alias;
my $table3 = $dbi->table3;
my $key1 = $dbi->key1;
my $key2 = $dbi->key2;
my $key3 = $dbi->key3;
my $key4 = $dbi->key4;
my $key5 = $dbi->key5;
my $key6 = $dbi->key6;
my $key7 = $dbi->key7;
my $key8 = $dbi->key8;
my $key9 = $dbi->key9;
my $key10 = $dbi->key10;
my $create_table1 = $dbi->create_table1;
my $create_table1_2 = $dbi->create_table1_2;
my $create_table1_type = $dbi->create_table1_type;
my $create_table1_highperformance = $dbi->create_table1_highperformance;
my $create_table2 = $dbi->create_table2;
my $create_table2_2 = $dbi->create_table2_2;
my $create_table3 = $dbi->create_table3;
my $create_table_reserved = $dbi->create_table_reserved;
my ($q, $p) = $dbi->_qp;
my $date_typename = $dbi->date_typename;
my $datetime_typename = $dbi->datetime_typename;
my $date_datatype = $dbi->date_datatype;
my $datetime_datatype = $dbi->datetime_datatype;

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
my $user_table_info;
my $user_column_info;
my $values_clause;
my $assign_clause;
my $reuse;
my $affected;
my $dbi1;
my $dbi2;
my $dbi3;
my $dbi4;
my $dbi5;
my $pool;

require MyDBI1;
{
  package MyDBI4;

  use strict;
  use warnings;

  use base 'DBIx::Custom';

  sub connect {
    my $self = shift->SUPER::connect(@_);
    
    $self->include_model(
      MyModel2 => [
          $table1,
          {class => $table2, name => $table2}
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
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }

  package MyModel2::table2;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }

  package MyModel2::TABLE1;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }

  package MyModel2::TABLE2;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }


  package MyModel2::main::table1;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }

  package MyModel2::main::table2;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }

  package MyModel2::dbix_custom::table1;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
  }

  sub list { shift->select; }

  package MyModel2::dbix_custom::table2;

  use strict;
  use warnings;

  use base 'MyModel2::Base1';

  sub insert {
    my ($self, $param) = @_;
    
    return $self->SUPER::insert($param);
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
    
    $self->include_model('MyModel8::MyModel1');
    
    return $self;
  }
}

test 'execute reuse option';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$reuse = {};
for my $i (1 .. 2) {
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1, reuse => $reuse);
}
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 1, $key2 => 2}]);
ok(keys %$reuse);
ok((keys %$reuse)[0] !~ /\?/);

# Get user table info
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
eval { $dbi->execute("drop table $table3") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->execute($create_table3);
$user_table_info = $dbi->get_table_info(exclude => $dbi->exclude_table);

# Create table
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$model = $dbi->create_model(table => $table1);
$model->insert({$key1 => 1, $key2 => 2});
is_deeply($model->select->all, [{$key1 => 1, $key2 => 2}]);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$model = $dbi->create_model(table => $table1);
$model->insert({$key1 => 1, $key2 => 2});
is_deeply($model->select($key1)->all, [{$key1 => 1}]);

test 'DBIx::Custom::Result test';
$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$source = "select $key1, $key2 from $table1";
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
is_deeply(\@rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "fetch_hash");

$result = $dbi->execute($query);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]]);

$result = $dbi->execute($query);
$rows = $result->fetch_hash_all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "all");

is_deeply($dbi->select($key1, table => $table1)->values, [1, 3]);

is($dbi->select('count(*)', table => $table1)->value, 2);
ok(!defined $dbi->select($key1, table => $table1, where => {$key1 => 10})->value);

test 'Named placeholder';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);

$source = "select * from $table1 where $key1 = :$key1 and $key2 = :$key2";
$result = $dbi->execute($source, {$key1 => 1, $key2 => 2});
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

$source = "select * from $table1 where $key1 = \n:$key1\n and $key2 = :$key2";
$result = $dbi->execute($source, {$key1 => 1, $key2 => 2});
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

$source = "select * from $table1 where $key1 = :$key1 or $key1 = :$key1";
$result = $dbi->execute($source, {$key1 => [1, 2]});
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

$source = "select * from $table1 where $key1 = :$table1.$key1 and $key2 = :$table1.$key2";
$result = $dbi->execute(
  $source,
  {"$table1.$key1" => 1, "$table1.$key2" => 1},
  filter => {"$table1.$key2" => sub { $_[0] * 2 }}
);
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => '2011-10-14 12:19:18', $key2 => 2}, table => $table1);
$source = "select * from $table1 where $key1 = '2011-10-14 12:19:18' and $key2 = :$key2";
$result = $dbi->execute(
  $source,
  {$key2 => 2},
);

$rows = $result->all;
like($rows->[0]->{$key1}, qr/2011-10-14 12:19:18/);
is($rows->[0]->{$key2}, 2);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 'a:b c:d', $key2 => 2}, table => $table1);
$source = "select * from $table1 where $key1 = 'a\\:b c\\:d' and $key2 = :$key2";
$result = $dbi->execute(
  $source,
  {$key2 => 2},
);
$rows = $result->all;
is_deeply($rows, [{$key1 => 'a:b c:d', $key2 => 2}]);

test 'Error case';
eval {DBIx::Custom->connect(dsn => 'dbi:SQLit')};
ok($@, "connect error");

eval{$dbi->execute("{p }", {}, query => 1)};
ok($@, "create_query invalid SQL template");

test 'insert';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert(table => $table1, param => {$key1 => 1, $key2 => 2});
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "basic");

$dbi->execute("delete from $table1");
$dbi->register_filter(
  three_times => sub { $_[0] * 3 }
);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1, filter => {$key1 => 'three_times'});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 3, $key2 => 2}], "filter");
$dbi->delete_all(table => $table1);


eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1, append => '   ');
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}], 'insert append');

eval{$dbi->insert({';' => 1}, table => 'table')};
like($@, qr/safety/);

eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert({select => 1}, table => 'table');
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => undef}], "reserved word");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => \"'1'", $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1,
wrap => {$key1 => sub { "$_[0] - 1" }});
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 0, $key2 => 2}, {$key1 => 3, $key2 => 4}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert_timestamp(
  $key1 => '5'
);
$dbi->insert({$key2 => 2}, table => $table1, timestamp => 1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 5, $key2 => 2}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert_timestamp(
  [$key1, $key2] => sub { 5 }
);
$dbi->insert(table => $table1, timestamp => 1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 5, $key2 => 5}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert_timestamp(
  [$key1, $key2] => sub { "" . DBIx::Custom->new }
);
$dbi->insert(table => $table1, timestamp => 1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is($rows->[0]->{$key1}, $rows->[0]->{$key2});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, created_at => $key2);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, ctime => $key2);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, updated_at => $key3);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, mtime => $key3);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, created_at => $key2, updated_at => $key3);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, created_at => $key2, mtime => $key3);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, ctime => $key2, updated_at => $key3);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$dbi->insert($param, table => $table1, ctime => $key2, mtime => $key3);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$model = $dbi->create_model(table => $table1, created_at => $key2);
$param = {$key1 => 1};
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$model = $dbi->create_model(table => $table1, ctime => $key2);
$param = {$key1 => 1};
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$model = $dbi->create_model(table => $table1, updated_at => $key3);
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$model = $dbi->create_model(table => $table1, mtime => $key3);
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$model = $dbi->create_model(table => $table1, created_at => $key2, updated_at => $key3);
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$model = $dbi->create_model(table => $table1, created_at => $key2, mtime => $key3);
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$model = $dbi->create_model(table => $table1, ctime=> $key2, updated_at => $key3);
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1};
$model = $dbi->create_model(table => $table1, ctime=> $key2, mtime => $key3);
$model->insert($param);
$result = $dbi->select(table => $table1);
is_deeply($param, {$key1 => 1});
$row   = $result->one;
is($row->{$key1}, 1);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
like($row->{$key3}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
is($row->{$key2}, $row->{$key3});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert([{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}] , table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert([{$key1 => 1}, {$key1 => 3}] ,
  table => $table1,
  updated_at => $key2,
  created_at => $key3
);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is($rows->[0]->{$key1}, 1);
is($rows->[1]->{$key1}, 3);
like($rows->[0]->{$key2}, qr/\d{2}:/);
like($rows->[1]->{$key2}, qr/\d{2}:/);
like($rows->[0]->{$key3}, qr/\d{2}:/);
like($rows->[1]->{$key3}, qr/\d{2}:/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert([{$key1 => 1}, {$key1 => 3}] ,
  table => $table1,
  mtime => $key2,
  ctime => $key3
);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is($rows->[0]->{$key1}, 1);
is($rows->[1]->{$key1}, 3);
like($rows->[0]->{$key2}, qr/\d{2}:/);
like($rows->[1]->{$key2}, qr/\d{2}:/);
like($rows->[0]->{$key3}, qr/\d{2}:/);
like($rows->[1]->{$key3}, qr/\d{2}:/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert([{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}] ,
table => $table1, filter => {$key1 => sub { $_[0] * 2 }});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 2, $key2 => 2}, {$key1 => 6, $key2 => 4}], "basic");

test 'update_or_insert';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->update_or_insert(
  {$key2 => 2},
  table => $table1,
  primary_key => $key1,
  id => 1
);
$row = $dbi->select(id => 1, table => $table1, primary_key => $key1)->one;
is_deeply($row, {$key1 => 1, $key2 => 2}, "basic");

$dbi->update_or_insert(
  {$key2 => 3},
  table => $table1,
  primary_key => $key1,
  id => 1
);
$rows = $dbi->select(id => 1, table => $table1, primary_key => $key1)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 3}], "basic");

eval {
  $dbi->update_or_insert(
    {$key2 => 3},
    table => $table1,
  );
};

like($@, qr/primary_key/);

eval {
  $dbi->insert({$key1 => 1}, table => $table1);
  $dbi->update_or_insert(
    {$key2 => 3},
    table => $table1,
    primary_key => $key1,
    id => 1
  );
};
like($@, qr/one/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->update_or_insert(
  {},
  table => $table1,
  primary_key => $key1,
  id => 1
);
$row = $dbi->select(id => 1, table => $table1, primary_key => $key1)->one;
is($row->{$key1}, 1);

eval { 
  $affected = $dbi->update_or_insert(
    {},
    table => $table1,
    primary_key => $key1,
    id => 1
  );
};
is($affected, 0);

test 'model update_or_insert';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$model = $dbi->create_model(
  table => $table1,
  primary_key => $key1
);
$model->update_or_insert({$key2 => 2}, id => 1);
$row = $model->select(id => 1)->one;
is_deeply($row, {$key1 => 1, $key2 => 2}, "basic");

eval {
  $model->insert({$key1 => 1});
  $model->update_or_insert(
    {$key2 => 3},
    id => 1
  );
};
like($@, qr/one/);

test 'bind filter';
$dbi->execute("delete from $table1");
$dbi->register_filter(
  three_times => sub { $_[0] * 3 }
);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1, filter => {$key1 => 'three_times'});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 3, $key2 => 2}], "filter");

test 'update';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->update({$key2 => 11}, table => $table1, where => {$key1 => 1});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->update(param => {$key2 => 11}, table => $table1, where => {$key1 => 1});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");
                
$dbi->execute("delete from $table1");
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->update({$key2 => 12}, table => $table1, where => {$key2 => 2, $key3 => 3});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 12, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "update key same as search key");

$dbi->update({$key2 => [12]}, table => $table1, where => {$key2 => 2, $key3 => 3});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 12, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "update key same as search key : param is array ref");

$dbi->execute("delete from $table1");
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update({$key2 => 11}, table => $table1, where => {$key1 => 1},
            filter => {$key2 => sub { $_[0] * 2 }});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 22, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "filter");

$result = $dbi->update({$key2 => 11}, table => $table1, where => {$key1 => 1}, append => '   ');

eval{$dbi->update(table => $table1)};
like($@, qr/where/, "not contain where");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$where = $dbi->where;
$where->clause(['and', "$key1 = :$key1", "$key2 = :$key2"]);
$where->param({$key1 => 1, $key2 => 2});
$dbi->update({$key1 => 3}, table => $table1, where => $where);
$result = $dbi->select(table => $table1);
is_deeply($result->all, [{$key1 => 3, $key2 => 2}], 'update() where');

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->update(
  {$key1 => 3},
  table => $table1,
  where => [
    ['and', "$key1 = :$key1", "$key2 = :$key2"],
    {$key1 => 1, $key2 => 2}
  ]
);
$result = $dbi->select(table => $table1);
is_deeply($result->all, [{$key1 => 3, $key2 => 2}], 'update() where');

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$where = $dbi->where;
$where->clause(['and', "$key2 = :$key2"]);
$where->param({$key2 => 2});
$dbi->update({$key1 => 3}, table => $table1, where => $where);
$result = $dbi->select(table => $table1);
is_deeply($result->all, [{$key1 => 3, $key2 => 2}], 'update() where');

eval{$dbi->update({';' => 1}, table => $table1, where => {$key1 => 1})};
like($@, qr/safety/);

eval{$dbi->update({$key1 => 1}, table => $table1, where => {';' => 1})};
like($@, qr/safety/);

eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->apply_filter('table', update => {out => sub { $_[0] * 3}});
$dbi->insert({select => 1}, table => 'table');
$dbi->update({update => 2}, table => 'table', where => {select => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 6}], "reserved word");

eval {$dbi->update_all({';' => 2}, table => 'table') };
like($@, qr/safety/);

eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->apply_filter('table', update => {out => sub { $_[0] * 3}});
$dbi->insert({select => 1}, table => 'table');
$dbi->update({update => 2}, table => 'table', where => {'table.select' => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 6}], "reserved word");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->update({$key2 => 11}, table => $table1, where => {$key1 => 1});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->update({$key2 => 11}, table => $table1, where => {$key1 => 1},
wrap => {$key2 => sub { "$_[0] - 1" }});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 10, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->update({$key2 => \"'11'"}, table => $table1, where => {$key1 => 1});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->update_timestamp(
  $key1 => '5'
);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->update(table => $table1, timestamp => 1, where => {$key2 => 2});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 5, $key2 => 2}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->update_timestamp(
  [$key1, $key2] => sub { '5' }
);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->update_all(table => $table1, timestamp => 1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 5, $key2 => 5}], "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->update_timestamp(
  [$key1, $key2] => sub { "" . DBIx::Custom->new }
);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->update_all(table => $table1, timestamp => 1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is($rows->[0]->{$key1}, $rows->[0]->{$key2});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$param = {$key2 => 11};
$dbi->update($param, table => $table1, where => {$key1 => 1});
is_deeply($param, {$key2 => 11});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$param = {$key2 => 11};
$dbi->update($param, table => $table1, where => {$key2 => 2});
is_deeply($param, {$key2 => 11});
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key3 => 4};
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update($param, table => $table1, updated_at => $key2, where => {$key1 => 1});
$result = $dbi->select(table => $table1);
is_deeply($param, {$key3 => 4});
$row   = $result->one;
is($row->{$key3}, 4);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key3 => 4};
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update($param, table => $table1, mtime => $key2, where => {$key1 => 1});
$result = $dbi->select(table => $table1);
is_deeply($param, {$key3 => 4});
$row   = $result->one;
is($row->{$key3}, 4);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key3 => 4};
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update($param, table => $table1, updated_at => $key2, where => {$key3 => 3});
$result = $dbi->select(table => $table1);
is_deeply($param, {$key3 => 4});
$row   = $result->one;
is($row->{$key3}, 4);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key3 => 4};
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update($param, table => $table1, mtime => $key2, where => {$key3 => 3});
$result = $dbi->select(table => $table1);
is_deeply($param, {$key3 => 4});
$row   = $result->one;
is($row->{$key3}, 4);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$model = $dbi->create_model(table => $table1, updated_at => $key2);
$param = {$key3 => 4};
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$model->update($param, where => {$key1 => 1});
$result = $dbi->select(table => $table1);
is_deeply($param, {$key3 => 4});
$row   = $result->one;
is($row->{$key3}, 4);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$model = $dbi->create_model(table => $table1, mtime => $key2);
$param = {$key3 => 4};
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$model->update($param, where => {$key1 => 1});
$result = $dbi->select(table => $table1);
is_deeply($param, {$key3 => 4});
$row   = $result->one;
is($row->{$key3}, 4);
like($row->{$key2}, qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);

test 'update_all';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->update_all({$key2 => 10}, table => $table1, filter => {$key2 => 'twice'});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 20, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 20, $key3 => 8, $key4 => 9, $key5 => 10}],
  "filter");

test 'delete';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->delete(table => $table1, where => {$key1 => 1});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 3, $key2 => 4}], "basic");

$dbi->execute("delete from $table1");
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->delete(table => $table1, where => {$key2 => 1}, filter => {$key2 => 'twice'});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 3, $key2 => 4}], "filter");

$dbi->delete(table => $table1, where => {$key1 => 1}, append => '   ');

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->delete(table => $table1, where => {$key1 => 1, $key2 => 2});
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 3, $key2 => 4}], "delete multi key");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$where = $dbi->where;
$where->clause(['and', "$key1 = :$key1", "$key2 = :$key2"]);
$where->param({ke1 => 1, $key2 => 2});
$dbi->delete(table => $table1, where => $where);
$result = $dbi->select(table => $table1);
is_deeply($result->all, [{$key1 => 3, $key2 => 4}], 'delete() where');

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->delete(
  table => $table1,
  where => [
    ['and', "$key1 = :$key1", "$key2 = :$key2"],
    {ke1 => 1, $key2 => 2}
  ]
);
$result = $dbi->select(table => $table1);
is_deeply($result->all, [{$key1 => 3, $key2 => 4}], 'delete() where');

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->delete(table => $table1, where => {$key1 => 1}, prefix => '    ');
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [], "basic");

test 'delete error';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
eval{$dbi->delete(table => $table1)};
like($@, qr/where/, "where key-value pairs not specified");

eval{$dbi->delete(table => $table1, where => {';' => 1})};
like($@, qr/safety/);

$dbi = undef;
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert({select => 1}, table => 'table');
$dbi->delete(table => 'table', where => {select => 1});
$result = $dbi->execute("select * from ${q}table$p");
$rows   = $result->all;
is_deeply($rows, [], "reserved word");

test 'delete_all';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->delete_all(table => $table1);
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [], "basic");

test 'select';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2},
  {$key1 => 3, $key2 => 4}], "table");

$rows = $dbi->select(table => $table1, column => [$key1])->all;
is_deeply($rows, [{$key1 => 1}, {$key1 => 3}], "table and columns and where key");

$rows = $dbi->select(table => $table1, where => {$key1 => 1})->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}], "table and columns and where key");

$rows = $dbi->select(table => $table1, column => [$key1], where => {$key1 => 3})->all;
is_deeply($rows, [{$key1 => 3}], "table and columns and where key");

$dbi->register_filter(decrement => sub { $_[0] - 1 });
$rows = $dbi->select(table => $table1, where => {$key1 => 2}, filter => {$key1 => 'decrement'})
          ->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}], "filter");

eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
$rows = $dbi->select(
  table => [$table1, $table2],
  column => "$table1.$key1 as " . u("${table1}_$key1")
    . ", $table2.$key1 as " . u("${table2}_$key1") . ", $key2, $key3",
  where   => {"$table1.$key2" => 2},
  relation  => {"$table1.$key1" => "$table2.$key1"}
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 1, u"${table2}_$key1" => 1, $key2 => 2, $key3 => 5}], "relation : exists where");

$rows = $dbi->select(
  table => [$table1, $table2],
  column => ["$table1.$key1 as " . u("${table1}_$key1") . ", ${table2}.$key1 as " . u("${table2}_$key1"), $key2, $key3],
  relation  => {"$table1.$key1" => "$table2.$key1"}
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 1, u"${table2}_$key1" => 1, $key2 => 2, $key3 => 5}], "relation : no exists where");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
$dbi->execute($create_table_reserved);
$dbi->apply_filter('table', select => {out => sub { $_[0] * 2}});
$dbi->insert({select => 1, update => 2}, table => 'table');
$result = $dbi->select(table => 'table', where => {select => 1});
$rows   = $result->all;
is_deeply($rows, [{select => 2, update => 2}], "reserved word");

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$row = $dbi->select($key1, table => $table1)->one;
is_deeply($row, {$key1 => 1});

eval { $dbi->select(table => $table1, where => {';' => 1}) };
like($@, qr/safety/);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->insert({$key1 => 5, $key2 => 6}, table => $table1);

$rows = $dbi->select(table => $table1, where => {$key1 => [1, 5]})->all;
is_deeply($rows, [
  {$key1 => 1, $key2 => 2},
  {$key1 => 5, $key2 => 6}
], "table");

$rows = $dbi->select(table => $table1, where => {$key1 => []})->all;
is_deeply($rows, [], "table");

test 'fetch filter';
eval { $dbi->execute("drop table $table1") };
$dbi->register_filter(
  three_times => sub { $_[0] * 3 }
);
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$result = $dbi->select(table => $table1);
$result->filter({$key1 => 'three_times'});
$row = $result->one;
is_deeply($row, {$key1 => 3, $key2 => 2});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$result = $dbi->select(column => [$key1, $key1, $key2], table => $table1);
$result->filter({$key1 => 'three_times'});
$row = $result->fetch_one;
is_deeply($row, [3, 3, 2]);

test 'filters';
$dbi = DBIx::Custom->new;

is($dbi->filters->{decode_utf8}->(encode_utf8('あ')),
  'あ', "decode_utf8");

is($dbi->filters->{encode_utf8}->('あ'),
  encode_utf8('あ'), "encode_utf8");

test 'transaction1';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->begin_work;
$dbi->dbh->{AutoCommit} = 0;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->rollback;
$dbi->dbh->{AutoCommit} = 1;

$result = $dbi->select(table => $table1);
ok(! $result->fetch_one, "rollback");


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->begin_work;
$dbi->dbh->{AutoCommit} = 0;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$dbi->commit;
$dbi->dbh->{AutoCommit} = 1;
$result = $dbi->select(table => $table1);
is_deeply(scalar $result->all, [{$key1 => 1, $key2 => 2}, {$key1 => 2, $key2 => 3}],
  "commit");

test 'execute';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
{
  local $Carp::Verbose = 0;
  eval{$dbi->execute("select * frm $table1")};
  like($@, qr/\Qselect * frm $table1/, "fail prepare");
  like($@, qr/\.t /, "fail : not verbose");
}
{
  local $Carp::Verbose = 1;
  eval{$dbi->execute("select * frm $table1")};
  like($@, qr/Custom.*\.t /s, "fail : verbose");
}

$query = $dbi->execute("select * from $table1 where $key1 = :$key1", {}, query => 1);
$dbi->dbh->disconnect;
eval{$dbi->execute($query, {$key1 => {a => 1}})};
ok($@, "execute fail");

test 'transaction2';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);

$dbi->begin_work;

eval {
  $dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
  die "Error";
  $dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
};

$dbi->rollback if $@;

$result = $dbi->select(table => $table1);
$rows = $result->all;
is_deeply($rows, [], "rollback");

$dbi->begin_work;

eval {
  $dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
  $dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
};

$dbi->commit unless $@;

$result = $dbi->select(table => $table1);
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], "commit");

$dbi->dbh->{AutoCommit} = 0;
eval{ $dbi->begin_work };
ok($@, "exception");
$dbi->dbh->{AutoCommit} = 1;

test 'execute';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
{
  local $Carp::Verbose = 0;
  eval{$dbi->execute("select * frm $table1")};
  like($@, qr/\Qselect * frm $table1/, "fail prepare");
  like($@, qr/\.t /, "fail : not verbose");
}
{
  local $Carp::Verbose = 1;
  eval{$dbi->execute("select * frm $table1")};
  like($@, qr/Custom.*\.t /s, "fail : verbose");
}

$query = $dbi->execute("select * from $table1 where $key1 = :$key1", {}, query => 1);
$dbi->dbh->disconnect;
eval{$dbi->execute($query, {$key1 => {a => 1}})};
ok($@, "execute fail");

test 'method';
$dbi->helper(
  one => sub { 1 }
);
$dbi->helper(
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
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3});
$dbi->apply_filter(
  $table1, $key1 => {out => 'twice', in => 'three_times'}, 
            $key2 => {out => 'three_times', in => 'twice'});
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$result = $dbi->execute("select * from $table1");
$row   = $result->fetch_hash_one;
is_deeply($row, {$key1 => 2, $key2 => 6}, "insert");
$result = $dbi->select(table => $table1);
$row   = $result->one;
is_deeply($row, {$key1 => 6, $key2 => 12}, "insert");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3});
$dbi->apply_filter(
  $table1, $key1 => {out => 'twice', in => 'three_times'}, 
            $key2 => {out => 'three_times', in => 'twice'});
$dbi->apply_filter(
  $table1, $key1 => {out => undef}
); 
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$result = $dbi->execute("select * from $table1");
$row   = $result->one;
is_deeply($row, {$key1 => 1, $key2 => 6}, "insert");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
  $table1, $key1 => {out => 'twice', in => 'twice'}
);
$dbi->insert({$key1 => 1, $key2 => 2},table => $table1, filter => {$key1 => undef});
$dbi->update({$key1 => 2}, table => $table1, where => {$key2 => 2});
$result = $dbi->execute("select * from $table1");
$row   = $result->one;
is_deeply($row, {$key1 => 4, $key2 => 2}, "update");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
  $table1, $key1 => {out => 'twice', in => 'twice'}
);
$dbi->insert({$key1 => 2, $key2 => 2}, table => $table1, filter => {$key1=> undef});
$dbi->delete(table => $table1, where => {$key1 => 1});
$result = $dbi->execute("select * from $table1");
$rows   = $result->all;
is_deeply($rows, [], "delete");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
  $table1, $key1 => {out => 'twice', in => 'twice'}
);
$dbi->insert({$key1 => 2, $key2 => 2}, table => $table1, filter => {$key1 => undef});
$result = $dbi->select(table => $table1, where => {$key1 => 1});
$result->filter({$key2 => 'twice'});
$rows   = $result->all;
is_deeply($rows, [{$key1 => 4, $key2 => 4}], "select");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->apply_filter(
  $table1, $key1 => {out => 'twice', in => 'twice'}
);
$dbi->insert({$key1 => 2, $key2 => 2}, table => $table1, filter => {$key1 => undef});
$result = $dbi->execute("select * from $table1 where $key1 = :$key1 and $key2 = :$key2",
  {$key1 => 1, $key2 => 2},
  table => [$table1]);
$rows   = $result->all;
is_deeply($rows, [{$key1 => 4, $key2 => 2}], "execute");


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->register_filter(three_times => sub { $_[0] * 3 });
$dbi->apply_filter(
  $table1, $key2 => {out => 'twice', in => 'twice'}
);
$dbi->apply_filter(
  $table2, $key3 => {out => 'three_times', in => 'three_times'}
);
$dbi->insert({$key1 => 5, $key2 => 2}, table => $table1, filter => {$key2 => undef});
$dbi->insert({$key1 => 5, $key3 => 6}, table => $table2, filter => {$key3 => undef});
$result = $dbi->select(
  table => [$table1, $table2],
  column => [$key2, $key3],
  where => {"$table1.$key2" => 1, "$table2.$key3" => 2}, relation => {"$table1.$key1" => "$table2.$key1"});

$result->filter({$key2 => 'twice'});
$rows   = $result->all;
is_deeply($rows, [{$key2 => 4, $key3 => 18}], "select : join");

$result = $dbi->select(
  table => [$table1, $table2],
  column => [$key2, $key3],
  where => {$key2 => 1, $key3 => 2}, relation => {"$table1.$key1" => "$table2.$key1"});

$result->filter({$key2 => 'twice'});
$rows   = $result->all;
is_deeply($rows, [{$key2 => 4, $key3 => 18}], "select : join : omit");

test 'connect super';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
is($dbi->select(table => $table1)->one->{$key1}, 1);

$dbi = DBIx::Custom->new;
$dbi->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
is($dbi->select(table => $table1)->one->{$key1}, 1);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
is($dbi->select(table => $table1)->one->{$key1}, 1);

test 'empty where select';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$result = $dbi->select(table => $table1, where => {});
$row = $result->one;
is_deeply($row, {$key1 => 1, $key2 => 2});

test 'select query option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$query = $dbi->insert({$key1 => 1, $key2 => 2}, table => $table1, query => 1);
ok(ref $query);
$query = $dbi->update({$key2 => 2}, table => $table1, where => {$key1 => 1}, query => 1);
ok(ref $query);
$query = $dbi->delete(table => $table1, where => {$key1 => 1}, query => 1);
ok(ref $query);
$query = $dbi->select(table => $table1, where => {$key1 => 1, $key2 => 2}, query => 1);
ok(ref $query);

test 'where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$where = $dbi->where->clause(['and', "$key1 = :$key1", "$key2 = :$key2"]);
is("$where", "where ( $key1 = :$key1 and $key2 = :$key2 )", 'no param');

$where = $dbi->where
  ->clause(['and', "$key1 = :$key1", "$key2 = :$key2"])
  ->param({$key1 => 1});

$result = $dbi->select(
  table => $table1,
  where => $where
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$result = $dbi->select(
  table => $table1,
  where => [
    ['and', "$key1 = :$key1", "$key2 = :$key2"],
    {$key1 => 1}
  ]
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where
  ->clause(['and', "$key1 = :$key1", "$key2 = :$key2"])
  ->param({$key1 => 1, $key2 => 2});
$result = $dbi->select(
  table => $table1,
  where => $where
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where
  ->clause(['and', "$key1 = :$key1", "$key2 = :$key2"])
  ->param({});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$where = $dbi->where
  ->clause(['and', ['or', "$key1 > :$key1", "$key1 < :$key1"], "$key2 = :$key2"])
  ->param({$key1 => [0, 3], $key2 => 2});
$result = $dbi->select(
  table => $table1,
  where => $where,
); 
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where;
$result = $dbi->select(
  table => $table1,
  where => $where
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

eval {
$where = $dbi->where
           ->clause(['uuu']);
$result = $dbi->select(
  table => $table1,
  where => $where
);
};
ok($@);

$where = $dbi->where;
is("$where", '');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 2])
  ->param({$key1 => [1, 3]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$where = $dbi->where
   ->clause(['or', ("$key1 = :$key1") x 2])
   ->param({$key1 => [1]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 2])
  ->param({$key1 => 1});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where
  ->clause("$key1 = :$key1")
  ->param({$key1 => 1});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [$dbi->not_exists, 1, 3]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [1, $dbi->not_exists, 3]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [1, 3, $dbi->not_exists]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [1, $dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [$dbi->not_exists, 1, $dbi->not_exists]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [$dbi->not_exists, $dbi->not_exists, 1]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => [$dbi->not_exists, $dbi->not_exists, $dbi->not_exists]});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], 'not_exists');

$where = $dbi->where
  ->clause(['or', ("$key1 = :$key1") x 3])
  ->param({$key1 => []});
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], 'not_exists');

$where = $dbi->where
  ->clause(['and',"$key1 is not null", "$key2 is not null" ]);
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}], 'not_exists');

eval {$dbi->where(ppp => 1) };
like($@, qr/invalid/);

$where = $dbi->where(
  clause => ['and', ['or'], ['and', "$key1 = :$key1", "$key2 = :$key2"]],
  param => {$key1 => 1, $key2 => 2}
);
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);


$where = $dbi->where(
  clause => ['and', ['or'], ['or', ":$key1", ":$key2"]],
  param => {}
);
$result = $dbi->select(
  table => $table1,
  where => $where,
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$where->param({$key1 => undef});
$result = $dbi->execute("select * from $table1 $where", {$key1 => 1});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where;
$where->clause(['or', ":${key1}{=}", ":${key1}{=}"]);
$where->param({$key1 => [undef, undef]});
$result = $dbi->execute("select * from $table1 $where", {$key1 => [1, 0]});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);
$result = $dbi->execute("select * from $table1 $where", {$key1 => [0, 1]});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => '00:00:00'}, table => $table1);
$dbi->insert({$key1 => 1, $key2 => '3'}, table => $table1);
$where = $dbi->where
  ->clause(['and', "$key1 = :$key1", "$key2 = '00:00:00'"])
  ->param({$key1 => 1});

$result = $dbi->select(
  table => $table1,
  where => $where
);
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => '00:00:00'}]);

test 'table not specify exception';
$dbi = DBIx::Custom->connect;
eval {$dbi->select($key1)};
ok($@);

test 'more tests';
$dbi = DBIx::Custom->connect;
eval{$dbi->apply_filter('table', 'column', [])};
like($@, qr/apply_filter/);

eval{$dbi->apply_filter('table', 'column', {outer => 2})};
like($@, qr/apply_filter/);

$dbi->apply_filter(

);
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->apply_filter($table1, $key2, 
                 {in => sub { $_[0] * 3 }, out => sub { $_[0] * 2 }});
$rows = $dbi->select(table => $table1, where => {$key2 => 1})->all;
is_deeply($rows, [{$key1 => 1, $key2 => 6}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->apply_filter($table1, $key2, {});
$rows = $dbi->select(table => $table1, where => {$key2 => 2})->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval {$dbi->apply_filter($table1, $key2, {out => 'no'})};
like($@, qr/not registered/);
eval {$dbi->apply_filter($table1, $key2, {in => 'no'})};
like($@, qr/not registered/);
$dbi->method({one => sub { 1 }});
is($dbi->one, 1);

eval{DBIx::Custom->connect(dsn => undef)};
like($@, qr/_connect/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(twice => sub { $_[0] * 2 });
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1,
           filter => {$key1 => 'twice'});
$row = $dbi->select(table => $table1)->one;
is_deeply($row, {$key1 => 2, $key2 => 2});
eval {$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1,
           filter => {$key1 => 'no'}) };
like($@, qr//);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->register_filter(one => sub { 1 });
$result = $dbi->select(table => $table1);
eval {$result->filter($key1 => 'no')};
like($@, qr/not registered/);

test 'option';
$dbi = DBIx::Custom->connect(option => {PrintError => 1});
ok($dbi->dbh->{PrintError});
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
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->delete_at(
  table => $table1,
  primary_key => [$key1, $key2],
  where => [1, 2],
);
is_deeply($dbi->select(table => $table1)->all, []);

$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->delete_at(
  table => $table1,
  primary_key => $key1,
  where => 1,
);
is_deeply($dbi->select(table => $table1)->all, []);

test 'insert_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert_at(
  {$key3 => 3},
  primary_key => [$key1, $key2], 
  table => $table1,
  where => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->insert_at(
  {$key2 => 2, $key3 => 3},
  primary_key => $key1, 
  table => $table1,
  where => 1,
);

is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert_at(
  {$key3 => 3},
  primary_key => [$key1, $key2], 
  table => $table1,
  where => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

test 'update_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update_at(
  {$key3 => 4},
  table => $table1,
  primary_key => [$key1, $key2],
  where => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 4);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update_at(
  {$key3 => 4},
  table => $table1,
  primary_key => $key1,
  where => 1,
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 4);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update_at(
  {$key3 => 4},
  table => $table1,
  primary_key => [$key1, $key2],
  where => [1, 2]
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 4);

test 'select_at';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->select_at(
  table => $table1,
  primary_key => [$key1, $key2],
  where => [1, 2]
);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->select_at(
  table => $table1,
  primary_key => $key1,
  where => 1,
);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->select_at(
  table => $table1,
  primary_key => [$key1, $key2],
  where => [1, 2]
);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

test 'model delete_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
eval { $dbi->execute("drop table $table3") };
$dbi->execute($create_table1_2);
$dbi->execute($create_table2_2);
$dbi->execute($create_table3);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->model($table1)->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => $table1)->all, []);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table2);
$dbi->model($table1)->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => $table1)->all, []);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table3);
$dbi->model($table3)->delete_at(where => [1, 2]);
is_deeply($dbi->select(table => $table3)->all, []);

test 'model insert_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->model($table1)->insert_at(
  {$key3 => 3},
  where => [1, 2],
);
$result = $dbi->model($table1)->select;
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

test 'model update_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->model($table1)->update_at(
  {$key3 => 4},
  where => [1, 2],
);
$result = $dbi->model($table1)->select;
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 4);

test 'model select_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->model($table1)->select_at(where => [1, 2]);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);


test 'mycolumn and column';
$dbi = MyDBI7->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->separator('__');
$dbi->setup_model;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->model($table1);
$result = $model->select(
  column => [$model->mycolumn, $model->column($table2)],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {$key1 => 1, $key2 => 2, u2"${table2}__$key1" => 1, u2"${table2}__$key3" => 3});
$result = $model->select(
  column => [$model->mycolumn, $model->column($table2 => '*')],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {$key1 => 1, $key2 => 2, u2"${table2}__$key1" => 1, u2"${table2}__$key3" => 3});
$result = $model->select(
  column => [
    {__MY__ => '*'},
    {$table2 => '*'}
  ],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {$key1 => 1, $key2 => 2, u2"${table2}__$key1" => 1, u2"${table2}__$key3" => 3});

$result = $model->select(
  column => [
    {__MY2__ => '*'},
    {$table2 => '*'}
  ],
  where => {"$table1.$key1" => 1},
  mytable_symbol => '__MY2__'
);
is_deeply($result->one,
        {$key1 => 1, $key2 => 2, u2"${table2}__$key1" => 1, u2"${table2}__$key3" => 3});

{
  my $original = $model->dbi->mytable_symbol;
  $model->dbi->mytable_symbol('__MY2__');
  $result = $model->select(
    column => [
      {__MY2__ => '*'},
      {$table2 => '*'}
    ],
    where => {"$table1.$key1" => 1},
  );
  is_deeply($result->one,
          {$key1 => 1, $key2 => 2, u2"${table2}__$key1" => 1, u2"${table2}__$key3" => 3});
  $model->dbi->mytable_symbol($original);
}

test 'values_clause';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1, $key2 => 2};
$values_clause = $dbi->values_clause($param);
$sql = <<"EOS";
insert into $table1 $values_clause
EOS
$dbi->execute($sql, $param, table => $table1);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key1 => 1, $key2 => 2};
$values_clause = $dbi->insert_param($param);
$sql = <<"EOS";
insert into $table1 $values_clause
EOS
$dbi->execute($sql, $param, table => $table1);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);

test 'mycolumn';
$dbi = MyDBI8->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->model($table1);
$result = $model->select_at(
  column => [
    $model->mycolumn,
    $model->column($table2)
  ]
);
is_deeply($result->one,
  {$key1 => 1, $key2 => 2, "$table2.$key1" => 1, "$table2.$key3" => 3});

$result = $model->select_at(
  column => [
    $model->mycolumn([$key1]),
    $model->column($table2 => [$key1])
  ]
);
is_deeply($result->one,
        {$key1 => 1, "$table2.$key1" => 1});
$result = $model->select_at(
  column => [
    $model->mycolumn([$key1]),
    {$table2 => [$key1]}
  ]
);
is_deeply($result->one,
        {$key1 => 1, "$table2.$key1" => 1});

$result = $model->select_at(
  column => [
    $model->mycolumn([$key1]),
    ["$table2.$key1", as => "$table2.$key1"]
  ]
);
is_deeply($result->one,
  {$key1 => 1, "$table2.$key1" => 1});

$result = $model->select_at(
  column => [
    $model->mycolumn([$key1]),
    ["$table2.$key1" => "$table2.$key1"]
  ]
);
is_deeply($result->one,
  {$key1 => 1, "$table2.$key1" => 1});

test 'merge_param';
$dbi = DBIx::Custom->new;
$params = [
  {$key1 => 1, $key2 => 2, $key3 => 3},
  {$key1 => 1, $key2 => 2},
  {$key1 => 1}
];
$param = $dbi->merge_param($params->[0], $params->[1], $params->[2]);
is_deeply($param, {$key1 => [1, 1, 1], $key2 => [2, 2], $key3 => 3});

$params = [
  {$key1 => [1, 2], $key2 => 1, $key3 => [1, 2]},
  {$key1 => [3, 4], $key2 => [2, 3], $key3 => 3}
];
$param = $dbi->merge_param($params->[0], $params->[1]);
is_deeply($param, {$key1 => [1, 2, 3, 4], $key2 => [1, 2, 3], $key3 => [1, 2, 3]});

test 'select() param option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 4}, table => $table2);
$dbi->insert({$key1 => 2, $key3 => 5}, table => $table2);
$rows = $dbi->select(
  table => $table1,
  column => "$table1.$key1 as " . u("${table1}_$key1") . ", $key2, $key3",
  where   => {"$table1.$key2" => 3},
  join  => ["inner join (select * from $table2 where :$table2.${key3}{=})" . 
            " $q$table2$p on $table1.$key1 = $q$table2$p.$key1"],
  param => {"$table2.$key3" => 5}
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 2, $key2 => 3, $key3 => 5}]);

$rows = $dbi->select(
  table => $table1,
  column => "$table1.$key1 as " . u("${table1}_$key1") . ", $key2, $key3",
  where   => {"$table1.$key2" => 3},
  join  => "inner join (select * from $table2 where :$table2.${key3}{=})" . 
           " $q$table2$p on $table1.$key1 = $q$table2$p.$key1",
  param => {"$table2.$key3" => 5}
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 2, $key2 => 3, $key3 => 5}]);

test 'select() string where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$rows = $dbi->select(
  table => $table1,
  where => "$key1 = :$key1 and $key2 = :$key2",
  where_param => {$key1 => 1, $key2 => 2}
)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$rows = $dbi->select(
  table => $table1,
  where => [
    "$key1 = :$key1 and $key2 = :$key2",
    {$key1 => 1, $key2 => 2}
  ]
)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$rows = $dbi->select(
  table => $table1,
  where => [
    "$key1 = :$key1 and $key2 = :$key2",
    {$key1 => 1, $key2 => 2}
  ]
)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}]);

test 'delete() string where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert(table => $table1, param => {$key1 => 2, $key2 => 3});
$dbi->delete(
  table => $table1,
  where => "$key1 = :$key1 and $key2 = :$key2",
  where_param => {$key1 => 1, $key2 => 2}
);
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 2, $key2 => 3}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$dbi->delete(
  table => $table1,
  where => [
    "$key1 = :$key1 and $key2 = :$key2",
     {$key1 => 1, $key2 => 2}
  ]
);
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 2, $key2 => 3}]);


test 'update() string where';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->update(
  {$key1 => 5},
  table => $table1,
  where => "$key1 = :$key1 and $key2 = :$key2",
  where_param => {$key1 => 1, $key2 => 2}
);
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 5, $key2 => 2}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->update(
  {$key1 => 5},
  table => $table1,
  where => [
    "$key1 = :$key1 and $key2 = :$key2",
    {$key1 => 1, $key2 => 2}
  ]
);
$rows = $dbi->select(table => $table1)->all;
is_deeply($rows, [{$key1 => 5, $key2 => 2}]);

test 'insert id and primary_key option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert(
  {$key3 => 3},
  primary_key => [$key1, $key2], 
  table => $table1,
  id => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi->insert(
  {$key3 => 3},
  primary_key => [$key1, $key2], 
  table => $table1,
  id => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi->delete_all(table => $table1);
$dbi->insert(
  {$key2 => 2, $key3 => 3},
  primary_key => $key1, 
  table => $table1,
  id => 0,
);

is($dbi->select(table => $table1)->one->{$key1}, 0);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert(
  {$key3 => 3},
  primary_key => $key1, 
  table => $table1,
  id => bless({value => 1}, 'AAAA'),
  filter => {$key1 => sub { shift->{value} }}
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert(
  {$key3 => 3},
  primary_key => [$key1, $key2], 
  table => $table1,
  id => 1,
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
ok(!$dbi->select(table => $table1)->one->{$key2});
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert(
  {$key3 => 3},
  primary_key => [$key1, $key2], 
  table => $table1,
  id => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 3);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key3 => 3, $key2 => 4};
$dbi->insert(
  $param,
  primary_key => [$key1, $key2], 
  table => $table1,
  id => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 4);
is($dbi->select(table => $table1)->one->{$key3}, 3);
is_deeply($param, {$key3 => 3, $key2 => 4});

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$param = {$key3 => 3, $key2 => 4};
$query = $dbi->insert(
  $param,
  primary_key => [$key1, $key2], 
  table => $table1,
  id => [1, 2],
  query => 1
);
ok(ref $query);
is_deeply($param, {$key3 => 3, $key2 => 4});

test 'model insert id and primary_key option';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->model($table1)->insert(
  {$key3 => 3},
  id => [1, 2],
);
$result = $dbi->model($table1)->select;
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->model($table1)->insert(
  {$key3 => 3},
  id => [1, 2]
);
$result = $dbi->model($table1)->select;
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

test 'update and id option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update(
  {$key3 => 4},
  table => $table1,
  primary_key => [$key1, $key2],
  id => [1, 2],
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 4);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 0, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update(
  {$key3 => 4},
  table => $table1,
  primary_key => $key1,
  id => 0,
);
is($dbi->select(table => $table1)->one->{$key1}, 0);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 4);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->update(
  {$key3 => 4},
  table => $table1,
  primary_key => [$key1, $key2],
  id => [1, 2]
);
is($dbi->select(table => $table1)->one->{$key1}, 1);
is($dbi->select(table => $table1)->one->{$key2}, 2);
is($dbi->select(table => $table1)->one->{$key3}, 4);


test 'model update and id option';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->model($table1)->update(
  {$key3 => 4},
  id => [1, 2],
);
$result = $dbi->model($table1)->select;
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 4);


test 'delete and id option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->delete(
  table => $table1,
  primary_key => [$key1, $key2],
  id => [1, 2],
);
is_deeply($dbi->select(table => $table1)->all, []);

$dbi->insert({$key1 => 0, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->delete(
  table => $table1,
  primary_key => $key1,
  id => 0,
);
is_deeply($dbi->select(table => $table1)->all, []);


test 'model delete and id option';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
eval { $dbi->execute("drop table $table3") };
$dbi->execute($create_table1_2);
$dbi->execute($create_table2_2);
$dbi->execute($create_table3);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$dbi->model($table1)->delete(id => [1, 2]);
is_deeply($dbi->select(table => $table1)->all, []);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table2);
$dbi->model($table1)->delete(id => [1, 2]);
is_deeply($dbi->select(table => $table1)->all, []);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table3);
$dbi->model($table3)->delete(id => [1, 2]);
is_deeply($dbi->select(table => $table3)->all, []);


test 'select and id option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->select(
  table => $table1,
  primary_key => [$key1, $key2],
  id => [1, 2]
);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 0, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->select(
  table => $table1,
  primary_key => $key1,
  id => 0,
);
$row = $result->one;
is($row->{$key1}, 0);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

$dbi->delete_all(table => $table1);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->select(
  table => $table1,
  primary_key => [$key1, $key2],
  id => [1, 2]
);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);


test 'model select_at';
$dbi = MyDBI6->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3}, table => $table1);
$result = $dbi->model($table1)->select(id => [1, 2]);
$row = $result->one;
is($row->{$key1}, 1);
is($row->{$key2}, 2);
is($row->{$key3}, 3);

test 'column separator is default .';
$dbi = MyDBI7->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->model($table1);
$result = $model->select(
  column => [$model->column($table2)],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {"$table2.$key1" => 1, "$table2.$key3" => 3});

$result = $model->select(
  column => [$model->column($table2 => [$key1, $key3])],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {"$table2.$key1" => 1, "$table2.$key3" => 3});

test 'separator';
$dbi = DBIx::Custom->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);

$dbi->create_model(
  table => $table1,
  join => [
   "left outer join $table2 on $table1.$key1 = $table2.$key1"
  ],
  primary_key => [$key1],
);
$model2 = $dbi->create_model(
  table => $table2,
);
$dbi->setup_model;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->model($table1);
$result = $model->select(
  column => [
    $model->mycolumn,
    {$table2 => [$key1, $key3]}
  ],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {$key1 => 1, $key2 => 2, "$table2.$key1" => 1, "$table2.$key3" => 3});
is_deeply($model2->select->one, {$key1 => 1, $key3 => 3});

$dbi->separator('__');
$model = $dbi->model($table1);
$result = $model->select(
  column => [
    $model->mycolumn,
    {$table2 => [$key1, $key3]}
  ],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
        {$key1 => 1, $key2 => 2, u2"${table2}__$key1" => 1, u2"${table2}__$key3" => 3});
is_deeply($model2->select->one, {$key1 => 1, $key3 => 3});

$dbi->separator('-');
$model = $dbi->model($table1);
$result = $model->select(
  column => [
    $model->mycolumn,
    {$table2 => [$key1, $key3]}
  ],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
  {$key1 => 1, $key2 => 2, hy"$table2-$key1" => 1, hy"$table2-$key3" => 3});
is_deeply($model2->select->one, {$key1 => 1, $key3 => 3});


test 'filter_off';
$dbi = DBIx::Custom->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);

$dbi->create_model(
  table => $table1,
  join => [
   "left outer join $table2 on $table1.$key1 = $table2.$key1"
  ],
  primary_key => [$key1],
);
$dbi->setup_model;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$model = $dbi->model($table1);
$result = $model->select(column => $key1);
$result->filter($key1 => sub { $_[0] * 2 });
is_deeply($result->one, {$key1 => 2});

test 'available_datetype';
$dbi = DBIx::Custom->connect;
ok($dbi->can('available_datatype'));


test 'select prefix option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$rows = $dbi->select(prefix => "$key1,", column => $key2, table => $table1)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}], "table");


test 'mapper';
$dbi = DBIx::Custom->connect;
$param = $dbi->mapper(param => {id => 1, author => 'Ken', price => 1900})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author" => sub { '%' . $_[0] . '%' }],
  price => {key => "$table1.price", condition => sub { $_[0] eq 1900 }}
);
is_deeply($param, {"$table1.id" => 1, "$table1.author" => '%Ken%',
"$table1.price" => 1900});

$dbi = DBIx::Custom->connect;
$param = $dbi->mapper(param => {id => 1, author => 'Ken', price => 1900})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author" => $dbi->like_value],
  price => {key => "$table1.price", condition => sub { $_[0] eq 1900 }}
);
is_deeply($param, {"$table1.id" => 1, "$table1.author" => '%Ken%',
"$table1.price" => 1900});

$param = $dbi->mapper(param => {id => 0, author => 0, price => 0})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author" => sub { '%' . $_[0] . '%' }],
  price => ["$table1.price", sub { '%' . $_[0] . '%' }, sub { $_[0] eq 0 }]
);
is_deeply($param, {"$table1.id" => 0, "$table1.author" => '%0%', "$table1.price" => '%0%'});

$param = $dbi->mapper(param => {id => '', author => '', price => ''})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author" => sub { '%' . $_[0] . '%' }],
  price => ["$table1.price", sub { '%' . $_[0] . '%' }, sub { $_[0] eq 1 }]
);
is_deeply($param, {});

$param = $dbi->mapper(param => {id => undef, author => undef, price => undef})->map(
  id => {key => "$table1.id"},
  price => {key => "$table1.price", condition => 'exists'}
);
is_deeply($param, {"$table1.price" => undef});

$param = $dbi->mapper(param => {price => 'a'})->map(
  id => {key => "$table1.id", condition => 'exists'},
  price => ["$table1.price", sub { '%' . $_[0] }, 'exists']
);
is_deeply($param, {"$table1.price" => '%a'});

$param = $dbi->mapper(param => {price => 'a'}, condition => 'exists')->map(
  id => {key => "$table1.id"},
  price => ["$table1.price", sub { '%' . $_[0] }]
);
is_deeply($param, {"$table1.price" => '%a'});

$param = $dbi->mapper(param => {price => 'a', author => 'b'})->map(
  price => sub { '%' . $_[0] },
  author => 'book.author'
);
is_deeply($param, {price => '%a', 'book.author' => 'b'});

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);

$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => undef}, condition => 'defined')->map;
$where->param($param);
$result = $dbi->execute("select * from $table1 $where", {$key1 => 1});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$where = $dbi->where;
$where->clause(['or', ":${key1}{=}", ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => [undef, undef]}, condition => 'exists')->map;
$result = $dbi->execute("select * from $table1 $where", {$key1 => [1, 0]});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);
$result = $dbi->execute("select * from $table1 $where", {$key1 => [0, 1]});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => [undef, undef]}, condition => 'defined')->map;
$where->param($param);
$result = $dbi->execute("select * from $table1 $where", {$key1 => [1, 0]});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);
$result = $dbi->execute("select * from $table1 $where", {$key1 => [0, 1]});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);


$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => 0}, condition => 'length')
->pass([$key1, $key2])->map;
$where->param($param);
$result = $dbi->execute("select * from $table1 $where", {$key1 => 1});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);

$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => ''}, condition => 'length')->map;
$where->param($param);
$result = $dbi->execute("select * from $table1 $where", {$key1 => 1});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => 5}, condition => sub { ($_[0] || '') eq 5 })
->pass([$key1, $key2])->map;
$where->param($param);
$result = $dbi->execute("select * from $table1 $where", {$key1 => 1});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}]);


$where = $dbi->where;
$where->clause(['and', ":${key1}{=}"]);
$param = $dbi->mapper(param => {$key1 => 7}, condition => sub { ($_[0] || '') eq 5 })->map;
$where->param($param);
$result = $dbi->execute("select * from $table1 $where", {$key1 => 1});
$row = $result->all;
is_deeply($row, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$where = $dbi->where;
$param = $dbi->mapper(param => {id => 1, author => 'Ken', price => 1900})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author", sub { '%' . $_[0] . '%' }],
  price => {key => "$table1.price", condition => sub { $_[0] eq 1900 }}
);
$where->param($param);
is_deeply($where->param, {"$table1.id" => 1, "$table1.author" => '%Ken%',
"$table1.price" => 1900});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => 0, author => 0, price => 0})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author", sub { '%' . $_[0] . '%' }],
  price => ["$table1.price", sub { '%' . $_[0] . '%' }, sub { $_[0] eq 0 }]
);
$where->param($param);
is_deeply($where->param, {"$table1.id" => 0, "$table1.author" => '%0%', "$table1.price" => '%0%'});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => '', author => '', price => ''})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author", sub { '%' . $_[0] . '%' }],
  price => ["$table1.price", sub { '%' . $_[0] . '%' }, sub { $_[0] eq 1 }]
);
$where->param($param);
is_deeply($where->param, {});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => undef, author => undef, price => undef}, condition => 'exists')->map(
  id => {key => "$table1.id"},
  price => {key => "$table1.price", condition => 'exists'}
);
is_deeply($param, {"$table1.id"  => undef,"$table1.price" => undef});

$where = $dbi->where;
$param = $dbi->mapper(param => {price => 'a'})->map(
  id => {key => "$table1.id", condition => 'exists'},
  price => ["$table1.price", sub { '%' . $_[0] }, 'exists']
);
is_deeply($param, {"$table1.price" => '%a'});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => [1, 2], author => 'Ken', price => 1900})->map(
  id => {key => "$table1.id"},
  author => ["$table1.author", sub { '%' . $_[0] . '%' }],
  price => {key => "$table1.price", condition => sub { $_[0] eq 1900 }}
);
is_deeply($param, {"$table1.id" => [1, 2], "$table1.author" => '%Ken%',
"$table1.price" => 1900});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => ['', ''], author => 'Ken', price => 1900}, condition => 'length')->map(
  id => {key => "$table1.id"},
  author => ["$table1.author", sub { '%' . $_[0] . '%' }],
  price => {key => "$table1.price", condition => sub { $_[0] eq 1900 }}
);
is_deeply($param, {"$table1.id" => [$dbi->not_exists, $dbi->not_exists], "$table1.author" => '%Ken%',
"$table1.price" => 1900});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => ['', ''], author => 'Ken', price => 1900})->map(
  id => {key => "$table1.id", condition => 'length'},
  author => ["$table1.author", sub { '%' . $_[0] . '%' }, 'defined'],
  price => {key => "$table1.price", condition => sub { $_[0] eq 1900 }}
);
is_deeply($param, {"$table1.id" => [$dbi->not_exists, $dbi->not_exists], "$table1.author" => '%Ken%',
"$table1.price" => 1900});

$where = $dbi->where;
$param = $dbi->mapper(param => {id => 'a', author => 'b', price => 'c'}, pass => [qw/id author/])
->map(price => {key => 'book.price'});
is_deeply($param, {id => 'a', author => 'b', 'book.price' => 'c'});

$param = $dbi->mapper(param => {author => 'Ken',})->map(
  author => ["$table1.author" => '%<value>%'],
);
is_deeply($param, {"$table1.author" => '%Ken%'});

$param = $dbi->mapper(param => {author => 'Ken'})->map(
  author => ["$table1.author" => 'p'],
);
is_deeply($param, {"$table1.author" => 'p'});

$param = $dbi->mapper(param => {author => 'Ken',})->map(
  author => {value => '%<value>%'}
);
is_deeply($param, {"author" => '%Ken%'});

test 'order';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 1}, table => $table1);
$dbi->insert({$key1 => 1, $key2 => 3}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 4}, table => $table1);
my $order = $dbi->order;
$order->prepend($key1, "$key2 desc");
$result = $dbi->select(table => $table1, append => $order);
is_deeply($result->all, [{$key1 => 1, $key2 => 3}, {$key1 => 1, $key2 => 1},
{$key1 => 2, $key2 => 4}, {$key1 => 2, $key2 => 2}]);
$order->prepend("$key1 desc");
$result = $dbi->select(table => $table1, append => $order);
is_deeply($result->all, [{$key1 => 2, $key2 => 4}, {$key1 => 2, $key2 => 2},
{$key1 => 1, $key2 => 3}, {$key1 => 1, $key2 => 1}]);

test 'DBIx::Custom header';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$result = $dbi->execute("select $key1 as h1, $key2 as h2 from $table1");
is_deeply([map { lc } @{$result->header}], [qw/h1 h2/]);

test 'Named placeholder :name(operater) syntax';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);

$source = "select * from $table1 where :${key1}{=} and :${key2}{=}";
$result = $dbi->execute($source, {$key1 => 1, $key2 => 2});
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

$source = "select * from $table1 where :${key1}{ = } and :${key2}{=}";
$result = $dbi->execute($source, {$key1 => 1, $key2 => 2});
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

$source = "select * from $table1 where :${key1}{<} and :${key2}{=}";
$result = $dbi->execute($source, {$key1 => 5, $key2 => 2});
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

$source = "select * from $table1 where :$table1.${key1}{=} and :$table1.${key2}{=}";
$result = $dbi->execute(
  $source,
  {"$table1.$key1" => 1, "$table1.$key2" => 1},
  filter => {"$table1.$key2" => sub { $_[0] * 2 }}
);
$rows = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}]);

test 'high perfomance way';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_highperformance);
$rows = [
  {$key7 => 1, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 7},
  {$key7 => 1, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 8},
];
{
  my $query;
  for my $row (@$rows) {
    $query ||= $dbi->insert($row, table => $table1, query => 1);
    $dbi->execute($query, $row, filter => {$key7 => sub { $_[0] * 2 }});
  }
  is_deeply($dbi->select(table => $table1)->all,
    [
      {$key7 => 2, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 7},
      {$key7 => 2, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 8},
    ]
  );
}

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_highperformance);
$rows = [
  {$key7 => 1, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 7},
  {$key7 => 1, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 8},
];
{
  my $query;
  my $sth;
  for my $row (@$rows) {
    $query ||= $dbi->insert($row, table => $table1, query => 1);
    $sth ||= $query->{sth};
    $sth->execute(map { $row->{$_} } sort keys %$row);
  }
  is_deeply($dbi->select(table => $table1)->all,
    [
      {$key7 => 1, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 7},
      {$key7 => 1, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => 8},
    ]
  );
}

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_highperformance);
$rows = [
  {$key7 => 10, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5},
  {$key7 => 11, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6},
];
{
  $model = $dbi->create_model(table => $table1, primary_key => $key1);
  my $query;
  for my $row (@$rows) {
    $query ||= $model->insert($row, query => 1);
    $model->execute($query, $row, filter => {$key7 => sub { $_[0] * 2 }});
  }
  is_deeply($dbi->select(table => $table1, append => 'order by key2')->all,
    [
      {$key7 => 20, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5, $key1 => undef},
      {$key7 => 22, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 6, $key1 => undef},
    ]
  );
}

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
{
  $ENV{DBIX_CUSTOM_DISABLE_MODEL_EXECUTE} = 1;
  $model = $dbi->create_model(table => $table1, primary_key => $key1);
  eval {$model->execute("select * from $table1 where :${key1}{=}", id => 1)};
  like($@, qr/primary_key/);
  delete $ENV{DBIX_CUSTOM_DISABLE_MODEL_EXECUTE};
}

{
  $ENV{DBIX_CUSTOM_DISABLE_MODEL_EXECUTE} = 1;
  $model = $dbi->create_model(table => $table1, primary_key => $key1);
  $model->insert({$key1 => 1});
  $result = $model->execute("select * from $table1 where :${key1}{=}", id => 1,
    table => $table1, primary_key => $key1);
  is($result->one->{$key1}, 1);
  delete $ENV{DBIX_CUSTOM_DISABLE_MODEL_EXECUTE};
}

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
{
  $model = $dbi->create_model(table => $table1, primary_key => $key1);
  $model->insert({$key1 => 1});
  eval {$result = $model->execute("select * from $table1 where :${key1}{=}", {}, id => 1)};
  is($result->one->{$key1}, 1);
}

test 'id option more';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_highperformance);
$row = {
  $key7 => 10, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5, $key1 => 2
};
$model = $dbi->create_model(table => $table1, primary_key => $key1);
$model->insert($row);
$query = $model->update({$key7 => 11}, id => 1, query => 1);
$model->execute($query, {$key7 => 11}, id => 1, filter => {"$table1.$key1" => sub { $_[0] * 2 }});
is_deeply($dbi->select(table => $table1)->one,
  {$key7 => 11, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5, $key1 => 2},
);

eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$model = $dbi->create_model(table => $table1, primary_key => $key1);
$model->insert({$key1 => 1, $key2 => 2});
$model = $dbi->create_model(table => $table2, primary_key => $key1,
  join => ["left outer join $table1 on $table2.$key1 = $table1.$key1"]);
$model->insert({$key1 => 1, $key3 => 3});
$result = $model->select(
  column => {$table1 => ["$key2"]},
  id => 1
);
is_deeply($result->all, [{"$table1.$key2" => 2}]);

eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_highperformance);
$row = {
  $key7 => 10, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5, $key1 => 2
};
$model = $dbi->create_model(table => $table1, primary_key => $key1);
$model->insert($row);
$query = $model->delete(id => 1, query => 1);
$model->execute($query, {}, id => 1, , filter => {"$table1.$key1" => sub { $_[0] * 2 }});
is_deeply($dbi->select(table => $table1)->all, []);

eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute($create_table1_highperformance) };
$row = {
  $key7 => 10, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5, $key1 => 2
};
$model = $dbi->create_model(table => $table1, primary_key => $key1);
$model->insert($row);
$query = $model->select(id => 1, query => 1);
$model->execute($query, {$key7 => 11}, id => 1, filter => {"$table1.$key1" => sub { $_[0] * 2 }});
$query = undef;
is_deeply($dbi->select(table => $table1)->one,
  {$key7 => 10, $key6 => 2, $key5 => 3, $key4 => 4, $key3 => 5, $key2 => 5, $key1 => 2},
);

test 'result';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);

$result = $dbi->select(table => $table1);
@rows = ();
while (my $row = $result->fetch) {
  push @rows, [@$row];
}
is_deeply(\@rows, [[1, 2], [3, 4]]);

$result = $dbi->select(table => $table1);
@rows = ();
while (my $row = $result->fetch_hash) {
  push @rows, {%$row};
}
is_deeply(\@rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);

test 'fetch_all';
$result = $dbi->select(table => $table1);
$rows = $result->fetch_all;
is_deeply($rows, [[1, 2], [3, 4]]);

$result = $dbi->select(table => $table1);
$rows = $result->fetch_hash_all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);

$result = $dbi->select(table => $table1);
$result->dbi->filters({three_times => sub { $_[0] * 3}});
$result->filter({$key1 => 'three_times'});
$rows = $result->fetch_all;
is_deeply($rows, [[3, 2], [9, 4]], "array");

$result = $dbi->select(column => [$key1, $key1, $key2], table => $table1);
$result->dbi->filters({three_times => sub { $_[0] * 3}});
$result->filter({$key1 => 'three_times'});
$rows = $result->fetch_all;
is_deeply($rows, [[3, 3, 2], [9, 9, 4]], "array");

$result = $dbi->select(table => $table1);
$result->dbi->filters({three_times => sub { $_[0] * 3}});
$result->filter({$key1 => 'three_times'});
$rows = $result->fetch_hash_all;
is_deeply($rows, [{$key1 => 3, $key2 => 2}, {$key1 => 9, $key2 => 4}], "hash");

test 'flat';
$result = $dbi->select(table => $table1);
$rows = [$result->flat];
is_deeply($rows, [1, 2, 3, 4]);

test 'kv';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 0, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);

$result = $dbi->select([$key1, $key2], table => $table1, append => "order by $key1");
$rows = $result->kv;
is_deeply($rows, {0 => {$key2 => 2}, 3 => {$key2 => 4}});

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 0, $key2 => 1}, table => $table1);
$dbi->insert({$key1 => 0, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 5}, table => $table1);

$result = $dbi->select([$key1, $key2], table => $table1, append => "order by $key2");
$rows = $result->kv(multi => 1);
is_deeply($rows, {
  0 => [
    {$key2 => 1},
    {$key2 => 2}
  ],
  3 => [
    {$key2 => 4},
    {$key2 => 5}
  ]
});

$result = $dbi->select([$key1, $key2], table => $table1, append => "order by $key2");
$rows = $result->kvs;
is_deeply($rows, {
  0 => [
    {$key2 => 1},
    {$key2 => 2}
  ],
  3 => [
    {$key2 => 4},
    {$key2 => 5}
  ]
});

test 'DBIx::Custom::Result fetch_multi';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->insert({$key1 => 5, $key2 => 6}, table => $table1);
$result = $dbi->select(table => $table1);
$rows = $result->fetch_multi(2);
is_deeply($rows, [[1, 2], [3, 4]]);
$rows = $result->fetch_multi(2);
is_deeply($rows, [[5, 6]]);
$rows = $result->fetch_multi(2);
ok(!$rows);

test 'DBIx::Custom::Result fetch_hash_multi';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
$dbi->insert({$key1 => 5, $key2 => 6}, table => $table1);
$result = $dbi->select(table => $table1);
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{$key1 => 1, $key2 => 2}, {$key1 => 3, $key2 => 4}]);
$rows = $result->fetch_hash_multi(2);
is_deeply($rows, [{$key1 => 5, $key2 => 6}]);
$rows = $result->fetch_hash_multi(2);
ok(!$rows);

test 'select() sqlfilter option';
$dbi = DBIx::Custom->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$rows = $dbi->select(
  table => $table1,
  column => $key1,
  sqlfilter => sub {
    my $sql = shift;
    $sql = "select * from ( $sql ) t where $key1 = 1";
    return $sql;
  }
)->all;
is_deeply($rows, [{$key1 => 1}]);

test 'select() after_build_sql option';
$dbi = DBIx::Custom->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 2, $key2 => 3}, table => $table1);
$rows = $dbi->select(
  table => $table1,
  column => $key1,
  after_build_sql => sub {
    my $sql = shift;
    $sql = "select * from ( $sql ) t where $key1 = 1";
    return $sql;
  }
)->all;
is_deeply($rows, [{$key1 => 1}]);

test 'dbi method from model';
$dbi = MyDBI9->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->setup_model;
$model = $dbi->model($table1);
eval{$model->execute("select * from $table1")};
ok(!$@);

test 'column table option';
$dbi = MyDBI9->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->setup_model;
$dbi->execute("insert into $table1 ($key1, $key2) values (1, 2)");
$dbi->execute("insert into $table2 ($key1, $key3) values (1, 4)");
$model = $dbi->model($table1);
$result = $model->select(
  column => [
    $model->column($table2, {alias => u$table2_alias})
  ],
  where => {u($table2_alias) . ".$key3" => 4}
);
is_deeply($result->one, 
        {u($table2_alias) . ".$key1" => 1, u($table2_alias) . ".$key3" => 4});

$dbi->separator('__');
$result = $model->select(
  column => [
    $model->column($table2, {alias => u$table2_alias})
  ],
  where => {u($table2_alias) . ".$key3" => 4}
);
is_deeply($result->one, 
  {u(${table2_alias}) . "__$key1" => 1, u(${table2_alias}) . "__$key3" => 4});

$dbi->separator('-');
$result = $model->select(
  column => [
    $model->column($table2, {alias => u$table2_alias})
  ],
  where => {u($table2_alias) . ".$key3" => 4}
);
is_deeply($result->one, 
  {u(${table2_alias}) . "-$key1" => 1, u(${table2_alias}) . "-$key3" => 4});

test 'create_model';
$dbi = DBIx::Custom->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);

$dbi->create_model(
  table => $table1,
  join => [
   "left outer join $table2 on $table1.$key1 = $table2.$key1"
  ],
  primary_key => [$key1]
);
$model2 = $dbi->create_model(
  table => $table2
);
$dbi->create_model(
  table => $table3,
  filter => [
    $key1 => {in => sub { uc $_[0] }}
  ]
);
$dbi->setup_model;
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->model($table1);
$result = $model->select(
  column => [$model->mycolumn, $model->column($table2)],
  where => {"$table1.$key1" => 1}
);
is_deeply($result->one,
  {$key1 => 1, $key2 => 2, "$table2.$key1" => 1, "$table2.$key3" => 3});
is_deeply($model2->select->one, {$key1 => 1, $key3 => 3});

test 'model method';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->create_model(
  table => $table2
);
$model->method(foo => sub { shift->select(@_) });
is_deeply($model->foo->one, {$key1 => 1, $key3 => 3});

test 'model helper';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 3}, table => $table2);
$model = $dbi->create_model(
  table => $table2
);
$model->helper(foo => sub { shift->select(@_) });
is_deeply($model->foo->one, {$key1 => 1, $key3 => 3});

test 'assign_clause';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);

$param = {$key2 => 11};
$assign_clause = $dbi->assign_clause($param);
$sql = <<"EOS";
update $table1 set $assign_clause
where $key1 = 1
EOS
$dbi->execute($sql, $param);
$result = $dbi->execute("select * from $table1 order by $key1", table => $table1);
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);

$param = {$key2 => 11, $key3 => 33};
$assign_clause = $dbi->assign_clause($param);
$sql = <<"EOS";
update $table1 set $assign_clause
where $key1 = 1
EOS
$dbi->execute($sql, $param);
$result = $dbi->execute("select * from $table1 order by $key1", table => $table1);
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 33, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);

$param = {$key2 => 11, $key3 => 33};
$assign_clause = $dbi->update_param($param, {no_set => 1});
$sql = <<"EOS";
update $table1 set $assign_clause
where $key1 = 1
EOS
$dbi->execute($sql, $param);
$result = $dbi->execute("select * from $table1 order by $key1", table => $table1);
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 33, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "update param no_set");

          
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_2);
$dbi->insert({$key1 => 1, $key2 => 2, $key3 => 3, $key4 => 4, $key5 => 5}, table => $table1);
$dbi->insert({$key1 => 6, $key2 => 7, $key3 => 8, $key4 => 9, $key5 => 10}, table => $table1);

$param = {$key2 => 11};
$assign_clause = $dbi->assign_param($param);
$sql = <<"EOS";
update $table1 set $assign_clause
where $key1 = 1
EOS
$dbi->execute($sql, $param, table => $table1);
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

$param = {$key2 => 11};
$assign_clause = $dbi->assign_clause($param);
$sql = <<"EOS";
update $table1 set $assign_clause
where $key1 = 1
EOS
$dbi->execute($sql, $param, table => $table1);
$result = $dbi->execute("select * from $table1 order by $key1");
$rows   = $result->all;
is_deeply($rows, [{$key1 => 1, $key2 => 11, $key3 => 3, $key4 => 4, $key5 => 5},
  {$key1 => 6, $key2 => 7,  $key3 => 8, $key4 => 9, $key5 => 10}],
  "basic");

test 'Model class';
$dbi = MyDBI1->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$model = $dbi->model($table1);
$model->insert({$key1 => 'a', $key2 => 'b'});
is_deeply($model->list->all, [{$key1 => 'a', $key2 => 'b'}], 'basic');
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$model = $dbi->model($table2);
$model->insert({$key1 => 'a'});
is_deeply($model->list->all, [{$key1 => 'a', $key3 => undef}], 'basic');
is($dbi->models->{$table1}, $dbi->model($table1));
is($dbi->models->{$table2}, $dbi->model($table2));

$dbi = MyDBI4->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$model = $dbi->model($table1);
$model->insert({$key1 => 'a', $key2 => 'b'});
is_deeply($model->list->all, [{$key1 => 'a', $key2 => 'b'}], 'basic');
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$model = $dbi->model($table2);
$model->insert({$key1 => 'a'});
is_deeply($model->list->all, [{$key1 => 'a', $key3 => undef}], 'basic');

$dbi = MyDBI5->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$model = $dbi->model($table2);
$model->insert({$key1 => 'a'});
is_deeply($model->list->all, [{$key1 => 'a', $key3 => undef}], 'include all model');
$dbi->insert({$key1 => 1}, table => $table1);
$model = $dbi->model($table1);
is_deeply($model->list->all, [{$key1 => 1, $key2 => undef}], 'include all model');

test 'primary_key';
$dbi = MyDBI1->connect;
$model = $dbi->model($table1);
$model->primary_key([$key1, $key2]);
is_deeply($model->primary_key, [$key1, $key2]);

test 'columns';
$dbi = MyDBI1->connect;
$model = $dbi->model($table1);
$model->columns([$key1, $key2]);
is_deeply($model->columns, [$key1, $key2]);

test 'columns';
$dbi = MyDBI1->connect;
$model = $dbi->model($table1);
$model->columns([$key1, $key2]);
is_deeply($model->columns, [$key1, $key2]);

test 'setup_model';
$dbi = MyDBI1->connect;
$dbi->user_table_info($user_table_info);
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };

$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->setup_model;
is_deeply([sort @{$dbi->model($table1)->columns}], [$key1, $key2]);
is_deeply([sort @{$dbi->model($table2)->columns}], [$key1, $key3]);

test 'each_column';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table ${q}table$p") };
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
eval { $dbi->execute("drop table $table3") };
$dbi->execute($create_table1_type);
$dbi->execute($create_table2);

$infos = [];
$dbi->each_column(sub {
  my ($self, $table, $column, $cinfo) = @_;
  
  if ($table =~ /^table\d/i) {
     my $info = [$table, $column, $cinfo->{COLUMN_NAME}];
     push @$infos, $info;
  }
});
$infos = [sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @$infos];
is_deeply($infos, 
  [
    [table_only($table1), $key1, $key1],
    [table_only($table1), $key2, $key2],
    [table_only($table2), $key1, $key1],
    [table_only($table2), $key3, $key3]
  ]
  
);

test 'each_table';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->execute($create_table1_type);

$infos = [];
$dbi->each_table(sub {
  my ($self, $table, $table_info) = @_;
  
  if ($table =~ /^table\d/i) {
    my $info = [$table, $table_info->{TABLE_NAME}];
    push @$infos, $info;
  }
});
$infos = [sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @$infos];
is_deeply($infos, 
  [
    [table_only($table1), table_only($table1)],
    [table_only($table2), table_only($table2)],
  ]
);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->execute($create_table1_type);

$infos = [];
$dbi->user_table_info($user_table_info);
$dbi->each_table(sub {
  my ($self, $table, $table_info) = @_;
  
  if ($table =~ /^table\d/i) {
     my $info = [$table, $table_info->{TABLE_NAME}];
     push @$infos, $info;
  }
});
$infos = [sort { $a->[0] cmp $b->[0] || $a->[1] cmp $b->[1] } @$infos];
is_deeply($infos, 
  [
    [table_only($table1), table_only($table1)],
    [table_only($table2), table_only($table2)],
    [table_only($table3), table_only($table3)],
  ]
);

test 'type_rule into';
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$user_column_info = $dbi->get_column_info(exclude_table => $dbi->exclude_table);


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);

$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => sub { '2010-' . $_[0] }
  }
);
$dbi->insert({$key1 => '01-01'}, table => $table1);
$result = $dbi->select(table => $table1);
like($result->one->{$key1}, qr/^2010-01-01/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => [
     [$date_typename, $datetime_typename] => sub {
        my $value = shift;
        $value =~ s/02/03/g;
        return $value;
     }
  ]
);
$dbi->insert({$key1 => '2010-01-02', $key2 => '2010-01-01 01:01:02'}, table => $table1);
$result = $dbi->select(table => $table1);
$row = $result->one;
like($row->{$key1}, qr/^2010-01-03/);
like($row->{$key2}, qr/^2010-01-01 01:01:03/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->insert({$key1 => '2010-01-03', $key2 => '2010-01-01 01:01:03'}, table => $table1);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => [
    [$date_typename, $datetime_typename] => sub {
      my $value = shift;
      $value =~ s/02/03/g;
      return $value;
    }
  ]
);
$result = $dbi->execute(
  "select * from $table1 where $key1 = :$key1 and $key2 = :$table1.$key2",
  {$key1 => '2010-01-03', "$table1.$key2" => '2010-01-01 01:01:02'}
);
$row = $result->one;
like($row->{$key1}, qr/^2010-01-03/);
like($row->{$key2}, qr/^2010-01-01 01:01:03/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->insert({$key1 => '2010-01-03', $key2 => '2010-01-01 01:01:03'}, table => $table1);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => [
    [$date_typename, $datetime_typename] => sub {
      my $value = shift;
      $value =~ s/02/03/g;
      return $value;
    }
  ]
);
$result = $dbi->execute(
  "select * from $table1 where $key1 = :$key1 and $key2 = :$table1.$key2",
  {$key1 => '2010-01-02', "$table1.$key2" => '2010-01-01 01:01:02'},
  table => $table1
);
$row = $result->one;
like($row->{$key1}, qr/^2010-01-03/);
like($row->{$key2}, qr/2010-01-01 01:01:03/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->register_filter(convert => sub {
  my $value = shift || '';
  $value =~ s/02/03/;
  return $value;
});
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => 'convert',
  },
  into1 => {
      $date_typename => 'convert',
  }
);
$dbi->insert({$key1 => '2010-02-02'}, table => $table1);
$result = $dbi->select(table => $table1);
like($result->fetch->[0], qr/^2010-03-03/);
$result = $dbi->select(column => [$key1, $key1], table => $table1);
$row = $result->fetch;
like($row->[0], qr/^2010-03-03/);
like($row->[1], qr/^2010-03-03/);

test 'type_rule and filter order';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/4/5/; return $v }
  },
  into2 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/5/6/; return $v }
  },
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/6/7/; return $v }
  },
  from2 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/7/8/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, 
table => $table1, filter => {$key1 => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }});
$result = $dbi->select(table => $table1);
$result->filter($key1 => sub { my $v = shift || ''; $v =~ s/8/9/; return $v });
like($result->fetch_one->[0], qr/^2010-01-09/);


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
  from2 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/4/5/; return $v }
  },
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
$dbi->user_column_info($user_column_info);
$result->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/6/; return $v }
  },
  from2 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/6/8/; return $v }
  }
);
$result->filter($key1 => sub { my $v = shift || ''; $v =~ s/8/9/; return $v });
like($result->fetch_one->[0], qr/^2010-01-09/);

test 'type_rule_off';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  },
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1, type_rule_off => 1);
$result = $dbi->select(table => $table1, type_rule_off => 1);
like($result->type_rule_off->fetch->[0], qr/^2010-01-03/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1, type_rule_off => 1);
$result = $dbi->select(table => $table1, type_rule_off => 1);
like($result->one->{$key1}, qr/^2010-01-04/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/4/5/; return $v }
  },
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
like($result->one->{$key1}, qr/^2010-01-05/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/4/5/; return $v }
  },
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
like($result->fetch->[0], qr/2010-01-05/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->register_filter(ppp => sub { my $v = shift || ''; $v =~ s/3/4/; return $v });
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => 'ppp'
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
like($result->one->{$key1}, qr/^2010-01-04/);

eval{$dbi->type_rule(
  into1 => {
    $date_typename => 'pp'
  }
)};
like($@, qr/not registered/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
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
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/4/5/; return $v }
  },
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
$result->type_rule_off;
like($result->one->{$key1}, qr/^2010-01-04/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/4/; return $v },
    $datetime_datatype => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
);
$dbi->insert({$key1 => '2010-01-03', $key2 => '2010-01-01 01:01:03'}, table => $table1);
$result = $dbi->select(table => $table1);
$result->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  }
);
$row = $result->one;
like($row->{$key1}, qr/^2010-01-05/);
like($row->{$key2}, qr/^2010-01-01 01:01:03/);

$result = $dbi->select(table => $table1);
$result->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  }
);
$row = $result->one;
like($row->{$key1}, qr/2010-01-05/);
like($row->{$key2}, qr/2010-01-01 01:01:03/);

$result = $dbi->select(table => $table1);
$result->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  }
);
$row = $result->one;
like($row->{$key1}, qr/2010-01-05/);
like($row->{$key2}, qr/2010-01-01 01:01:03/);

$result = $dbi->select(table => $table1);
$result->type_rule(
  from1 => [$date_datatype => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }]
);
$row = $result->one;
like($row->{$key1}, qr/2010-01-05/);
like($row->{$key2}, qr/2010-01-01 01:01:03/);

$dbi->register_filter(five => sub { my $v = shift || ''; $v =~ s/3/5/; return $v });
$result = $dbi->select(table => $table1);
$result->type_rule(
  from1 => [$date_datatype => 'five']
);
$row = $result->one;
like($row->{$key1}, qr/^2010-01-05/);
like($row->{$key2}, qr/^2010-01-01 01:01:03/);

$result = $dbi->select(table => $table1);
$result->type_rule(
  from1 => [$date_datatype => undef]
);
$row = $result->one;
like($row->{$key1}, qr/^2010-01-03/);
like($row->{$key2}, qr/^2010-01-01 01:01:03/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/4/; return $v },
  },
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
$result->filter($key1 => sub { my $v = shift || ''; $v =~ s/4/5/; return $v });
like($result->one->{$key1}, qr/^2010-01-05/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1);
$result = $dbi->select(table => $table1);
$result->filter($key1 => sub { my $v = shift || ''; $v =~ s/4/5/; return $v });
like($result->fetch->[0], qr/^2010-01-05/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
  into2 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  },
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/3/6/; return $v }
  },
  from2 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/(3|6)/7/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1, type_rule_off => 1);
$result = $dbi->select(table => $table1);
like($result->type_rule_off->fetch_one->[0], qr/^2010-01-03/);
$result = $dbi->select(table => $table1);
like($result->type_rule_on->fetch_one->[0], qr/^2010-01-07/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
  into2 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  },
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/(3|5)/6/; return $v }
  },
  from2 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/6/7/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1, type_rule1_off => 1);
$result = $dbi->select(table => $table1);
like($result->type_rule1_off->fetch_one->[0], qr/^2010-01-05/);
$result = $dbi->select(table => $table1);
like($result->type_rule1_on->fetch_one->[0], qr/^2010-01-07/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/5/; return $v }
  },
  into2 => {
    $date_typename => sub { my $v = shift || ''; $v =~ s/3/4/; return $v }
  },
  from1 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/5/6/; return $v }
  },
  from2 => {
    $date_datatype => sub { my $v = shift || ''; $v =~ s/(3|6)/7/; return $v }
  }
);
$dbi->insert({$key1 => '2010-01-03'}, table => $table1, type_rule2_off => 1);
$result = $dbi->select(table => $table1);
like($result->type_rule2_off->fetch_one->[0], qr/^2010-01-06/);
$result = $dbi->select(table => $table1);
like($result->type_rule2_on->fetch_one->[0], qr/^2010-01-07/);

test 'join';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
eval { $dbi->execute("drop table $table3") };
$dbi->execute("create table $table3 ($key3 int, $key4 int)");
$dbi->insert({$key3 => 5, $key4 => 4}, table => $table3);
$rows = $dbi->select(
  table => $table1,
  column => "$table1.$key1 as " . u("${table1}_$key1") . ", $table2.$key1 as " . u("${table2}_$key1") . ", $key2, $key3",
  where   => {"$table1.$key2" => 2},
  join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1"]
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 1, u"${table2}_$key1" => 1, $key2 => 2, $key3 => 5}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
eval { $dbi->execute("drop table $table3") };
$dbi->execute("create table $table3 ($key3 int, $key4 int)");
$dbi->insert({$key3 => 5, $key4 => 4}, table => $table3);
$rows = $dbi->select(
  table => $table1,
  column => "$table1.$key1 as " . u("${table1}_$key1") . ", $table2.$key1 as " . u("${table2}_$key1") . ", $key2, $key3",
  where   => {"$table1.$key2" => 2},
  join  => {
    clause => "left outer join $table2 on $table1.$key1 = $table2.$key1",
    table => [$table1, $table2]
  }
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 1, u"${table2}_$key1" => 1, $key2 => 2, $key3 => 5}]);

$rows = $dbi->select(
  table => $table1,
  where   => {$key1 => 1},
  join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1"]
)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}]);

$rows = $dbi->select(
  table => $table1,
  where   => {$key1 => 1},
  join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1",
            "left outer join $table3 on $table2.$key3 = $table3.$key3"]
)->all;
is_deeply($rows, [{$key1 => 1, $key2 => 2}]);

$rows = $dbi->select(
  column => "$table3.$key4 as " . u2("${table3}__$key4"),
  table => $table1,
  where   => {"$table1.$key1" => 1},
  join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1",
            "left outer join $table3 on $table2.$key3 = $table3.$key3"]
)->all;
is_deeply($rows, [{u2"${table3}__$key4" => 4}]);

$rows = $dbi->select(
  column => "$table1.$key1 as " . u2("${table1}__$key1"),
  table => $table1,
  where   => {"$table3.$key4" => 4},
  join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1",
            "left outer join $table3 on $table2.$key3 = $table3.$key3"]
)->all;
is_deeply($rows, [{u2"${table1}__$key1" => 1}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
$rows = $dbi->select(
  table => $table1,
  column => $dbi->_tq($table1) . ".${q}$key1$p as ${q}" . u("${table1}_$key1") . "$p, " . $dbi->_tq($table2) . ".${q}$key1$p as ${q}" . u("${table2}_$key1") . "$p, ${q}$key2$p, ${q}$key3$p",
  where   => {"$table1.$key2" => 2},
  join  => ["left outer join " . $dbi->_tq($table2) . " on " . $dbi->_tq($table1) . ".${q}$key1$p = " . $dbi->_tq($table2) . ".${q}$key1$p"],
)->all;
is_deeply($rows, [{u"${table1}_$key1" => 1, u"${table2}_$key1" => 1, $key2 => 2, $key3 => 5}],
  'quote');


$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$sql = <<"EOS";
left outer join (
select * from $table1 t1
  where t1.$key2 = (
    select max(t2.$key2) from $table1 t2
    where t1.$key1 = t2.$key1
  )
) $table3 on $table1.$key1 = $table3.$key1
EOS
$sql =~ s/\Q.table3/_table3/g;
$join = [$sql];
$rows = $dbi->select(
  table => $table1,
  column => u($table3) . ".$key1 as " . u2("${table3}__$key1"),
  join  => $join
)->all;
is_deeply($rows, [{u2"${table3}__$key1" => 1}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 4}, table => $table2);
$dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
$result = $dbi->select(
  table => $table1,
  join => [
    "left outer join $table2 on $table2.$key2 = '4' and $table1.$key1 = $table2.$key1"
  ]
);
is_deeply($result->all, [{$key1 => 1, $key2 => 2}]);
$result = $dbi->select(
  table => $table1,
  column => [{$table2 => [$key3]}],
  join => [
    "left outer join $table2 on $table2.$key3 = '4' and $table1.$key1 = $table2.$key1"
  ]
);
is_deeply($result->all, [{"$table2.$key3" => 4}]);
$result = $dbi->select(
  table => $table1,
  column => [{$table2 => [$key3]}],
  join => [
    "left outer join $table2 on $table1.$key1 = $table2.$key1 and $table2.$key3 = '4'"
  ]
);
is_deeply($result->all, [{"$table2.$key3" => 4}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 4}, table => $table2);
$dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
$result = $dbi->select(
  table => $table1,
  column => [{$table2 => [$key3]}],
  join => [
    {
      clause => "left outer join $table2 on $table2.$key3 = '4' and $table1.$key1 = $table2.$key1",
      table => [$table1, $table2]
    }
  ]
);
is_deeply($result->all, [{"$table2.$key3" => 4}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 4}, table => $table2);
$dbi->insert({$key1 => 1, $key3 => 1}, table => $table2);
$result = $dbi->select(
  table => $table1,
  column => [{$table2 => [$key3]}],
  join => [
    "left outer join $table2 on $table1.$key1 = $table2.$key1 and $table2.$key3 > '3'"
  ]
);
is_deeply($result->all, [{"$table2.$key3" => 4}]);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key3 => 4}, table => $table2);
$dbi->insert({$key1 => 1, $key3 => 1}, table => $table2);
$result = $dbi->select(
  table => $table1,
  column => [{$table2 => [$key3]}],
  join => [
    "left outer join $table2 on $table2.$key3 > '3' and $table1.$key1 = $table2.$key1"
  ]
);
is_deeply($result->all, [{"$table2.$key3" => 4}]);

test 'columns';
$dbi = MyDBI1->connect;
$model = $dbi->model($table1);

test 'count';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1);
$dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
$dbi->insert({$key1 => 1, $key2 => 3}, table => $table1);
is($dbi->count(table => $table1), 2);
is($dbi->count(table => $table1, where => {$key2 => 2}), 1);
$model = $dbi->create_model(table => $table1);
is($model->count, 2);

eval { $dbi->execute("drop table $table1") };
eval { $dbi->execute("drop table $table2") };
$dbi->execute($create_table1);
$dbi->execute($create_table2);
$model = $dbi->create_model(table => $table1, primary_key => $key1);
$model->insert({$key1 => 1, $key2 => 2});
$model = $dbi->create_model(table => $table2, primary_key => $key1,
  join => ["left outer join $table1 on $table2.$key1 = $table1.$key1"]);
$model->insert({$key1 => 1, $key3 => 3});
is($model->count(id => 1), 1);
is($model->count(where => {"$table2.$key3" => 3}), 1);

test 'table_alias option';
$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->insert({$key1 => '2010-01-01'}, table => $table1);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into1 => {
    $date_typename => sub { '2010-' . $_[0] }
  }
);
$result = $dbi->execute(
  "select * from $table1 TABLE1_ALIAS where :TABLE1_ALIAS.${key1}{=}",
  {"TABLE1_ALIAS.${key1}" => '01-01'},
  table_alias => {TABLE1_ALIAS => $table1}
);
like($result->one->{$key1}, qr/^2010-01-01/);

$dbi = DBIx::Custom->connect;
eval { $dbi->execute("drop table $table1") };
$dbi->execute($create_table1_type);
$dbi->insert({$key1 => '2010-01-01'}, table => $table1);
$dbi->user_column_info($user_column_info);
$dbi->type_rule(
  into2 => {
    $date_typename => sub { '2010-' . $_[0] }
  }
);
$result = $dbi->execute(
  "select * from $table1 TABLE1_ALIAS where :TABLE1_ALIAS.${key1}{=}",
  {"TABLE1_ALIAS.${key1}" => '01-01'},
  table_alias => {TABLE1_ALIAS => $table1}
);
like($result->one->{$key1}, qr/^2010-01-01/);

# DBIx::Custom::Where join
{
  my $dbi = DBIx::Custom->connect;
  eval { $dbi->execute("drop table $table1") };
  $dbi->execute($create_table1);
  $dbi->insert({$key1 => 1, $key2 => 2}, table => $table1);
  $dbi->insert({$key1 => 3, $key2 => 4}, table => $table1);
  eval { $dbi->execute("drop table $table2") };
  $dbi->execute($create_table2);
  $dbi->insert({$key1 => 1, $key3 => 5}, table => $table2);
  eval { $dbi->execute("drop table $table3") };
  $dbi->execute("create table $table3 ($key3 int, $key4 int)");
  $dbi->insert({$key3 => 5, $key4 => 4}, table => $table3);
  
  {
    my $where = $dbi->where;
    $where->param({$key1 => 1});
    $where->clause(":${key1}{=}");
    $where->join(["left outer join $table3 on $table2.$key3 = $table3.$key3"]);

    my $rows = $dbi->select(
      table => $table1,
      where   => $where,
      join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1"]
    )->all;
    is_deeply($rows, [{$key1 => 1, $key2 => 2}]);
  }
  {
    my $where = $dbi->where;
    $where->param({"$table1.$key1" => 1});
    $where->clause(":$table1.${key1}{=}");
    $where->join(["left outer join $table3 on $table2.$key3 = $table3.$key3"]);

    my $rows = $dbi->select(
      column => "$table3.$key4 as " . u2("${table3}__$key4"),
      table => $table1,
      where   => $where,
      join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1"]
    )->all;
    is_deeply($rows, [{u2"${table3}__$key4" => 4}]);
  }
  {
    my $where = $dbi->where;
    $where->param({"$table3.$key4" => 4});
    $where->clause(":$table3.${key4}{=}");
    $where->join(["left outer join $table3 on $table2.$key3 = $table3.$key3"]);

    my $rows = $dbi->select(
      column => "$table1.$key1 as " . u2("${table1}__$key1"),
      table => $table1,
      where   => $where,
      join  => ["left outer join $table2 on $table1.$key1 = $table2.$key1"]
    )->all;
    is_deeply($rows, [{u2"${table1}__$key1" => 1}]);
  }
}
