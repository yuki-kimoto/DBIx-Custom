use strict;
use warnings;

use Test::More 'no_plan';

use DBIx::Custom::QueryBuilder;

# Function for test name
my $test;
sub test{
    $test = shift;
}

# Variable for test
my $datas;
my $builder;
my $query;
my $ret_val;

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
    my $builder = DBIx::Custom::QueryBuilder->new;
    my $query = $builder->build_query($data->{source});
    is($query->{sql}, $data->{sql_expected}, "$test : $data->{name} : sql");
    is_deeply($query->{columns}, $data->{columns_expected}, "$test : $data->{name} : columns");
}


test 'Original tag processor';
$builder = DBIx::Custom::QueryBuilder->new;

$ret_val = $builder->register_tag_processor(
    p => sub {
        my @args = @_;
        
        my $expand    = "? $args[0] $args[1]";
        my $columns = [2];
        return [$expand, $columns];
    }
);

$query = $builder->build_query("{p a b}");
is($query->{sql}, "? a b;", "$test : register_tag_processor sql");
is_deeply($query->{columns}, [2], "$test : register_tag_processor columns");
isa_ok($ret_val, 'DBIx::Custom::QueryBuilder');


test "Tag processor error case";
$builder = DBIx::Custom::QueryBuilder->new;

eval{$builder->build_query('{? }')};
like($@, qr/\QColumn name must be specified in tag "{? }"/, "$test : ? not arguments");

eval{$builder->build_query("{a }")};
like($@, qr/\QTag "a" in "{a }" is not registered/, "$test : tag_processor not exist");

$builder->register_tag_processor({
    q => 'string'
});

eval{$builder->build_query("{q}", {})};
like($@, qr/Tag processor "q" must be sub reference/, "$test : tag_processor not code ref");

$builder->register_tag_processor({
   r => sub {} 
});

eval{$builder->build_query("{r}")};
like($@, qr/\QTag processor "r" must return [STRING, ARRAY_REFERENCE]/, "$test : tag processor return noting");

$builder->register_tag_processor({
   s => sub { return ["a", ""]} 
});

eval{$builder->build_query("{s}")};
like($@, qr/\QTag processor "s" must return [STRING, ARRAY_REFERENCE]/, "$test : tag processor return not array columns");

$builder->register_tag_processor(
    t => sub {return ["a", []]}
);


test 'General error case';
$builder = DBIx::Custom::QueryBuilder->new;
$builder->register_tag_processor(
    a => sub {
        return ["? ? ?", ['']];
    }
);
eval{$builder->build_query("{a}")};
like($@, qr/\QPlaceholder count in "? ? ?" must be same as column count 1/, "$test : placeholder count is invalid");


test 'Default tag processor Error case';
eval{$builder->build_query("{= }")};
like($@, qr/Column name must be specified in tag "{= }"/, "$test : basic '=' : key not exist");

eval{$builder->build_query("{in }")};
like($@, qr/Column name and count of values must be specified in tag "{in }"/, "$test : in : key not exist");

eval{$builder->build_query("{in a}")};
like($@, qr/\QColumn name and count of values must be specified in tag "{in }"/,
     "$test : in : key not exist");

eval{$builder->build_query("{in a r}")};
like($@, qr/\QColumn name and count of values must be specified in tag "{in }"/,
     "$test : in : key not exist");

