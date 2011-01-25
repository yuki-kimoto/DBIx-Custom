use strict;
use warnings;

use Test::More 'no_plan';

use DBIx::Custom;

# Function for test name
sub test{ print "# $_[0]\n" }

# Variable for test
my $datas;
my $builder;
my $query;
my $ret_val;
my $source;
my $tree;

test "Various source pattern";
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
    is_deeply($query->{columns}, $data->{columns_expected}, "$data->{name} : columns");
}


test 'Original tag';
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


test "Tag error case";
$builder = DBIx::Custom->new->query_builder;

eval{$builder->build_query('{? }')};
like($@, qr/\QColumn name must be specified in tag "{? }"/, "? not arguments");

eval{$builder->build_query("{a }")};
like($@, qr/\QTag "a" in "{a }" is not registered/, "tag not exist");

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
like($@, qr/\QPlaceholder count in "? ? ?" must be same as column count 1/, "placeholder count is invalid");


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


