use strict;
use warnings;

use Test::More 'no_plan';

use DBIx::Custom::SQLTemplate;

# Function for test name
my $test;
sub test{
    $test = shift;
}

# Variable for test
my $datas;
my $sql_tmpl;
my $query;
my $ret_val;

test "Various template pattern";
$datas = [
    # Basic tests
    {   name            => 'placeholder basic',
        tmpl            => "a {?  k1} b {=  k2} {<> k3} {>  k4} {<  k5} {>= k6} {<= k7} {like k8}", ,
        sql_expected    => "a ? b k2 = ? k3 <> ? k4 > ? k5 < ? k6 >= ? k7 <= ? k8 like ?;",
        columns_expected   => [qw/k1 k2 k3 k4 k5 k6 k7 k8/]
    },
    {
        name            => 'placeholder in',
        tmpl            => "{in k1 3};",
        sql_expected    => "k1 in (?, ?, ?);",
        columns_expected   => [qw/k1 k1 k1/]
    },
    
    # Table name
    {
        name            => 'placeholder with table name',
        tmpl            => "{= a.k1} {= a.k2}",
        sql_expected    => "a.k1 = ? a.k2 = ?;",
        columns_expected  => [qw/a.k1 a.k2/]
    },
    {   
        name            => 'placeholder in with table name',
        tmpl            => "{in a.k1 2} {in b.k2 2}",
        sql_expected    => "a.k1 in (?, ?) b.k2 in (?, ?);",
        columns_expected  => [qw/a.k1 a.k1 b.k2 b.k2/]
    },
    {
        name            => 'not contain tag',
        tmpl            => "aaa",
        sql_expected    => "aaa;",
        columns_expected  => [],
    }
];

for (my $i = 0; $i < @$datas; $i++) {
    my $data = $datas->[$i];
    my $sql_tmpl = DBIx::Custom::SQLTemplate->new;
    my $query = $sql_tmpl->create_query($data->{tmpl});
    is($query->{sql}, $data->{sql_expected}, "$test : $data->{name} : sql");
    is_deeply($query->{columns}, $data->{columns_expected}, "$test : $data->{name} : columns");
}


test 'Original tag processor';
$sql_tmpl = DBIx::Custom::SQLTemplate->new;

$ret_val = $sql_tmpl->register_tag_processor(
    p => sub {
        my ($tag_name, $args) = @_;
        
        my $expand    = "$tag_name ? $args->[0] $args->[1]";
        my $columns = [2];
        return ($expand, $columns);
    }
);

$query = $sql_tmpl->create_query("{p a b}");
is($query->{sql}, "p ? a b;", "$test : register_tag_processor sql");
is_deeply($query->{columns}, [2], "$test : register_tag_processor columns");
isa_ok($ret_val, 'DBIx::Custom::SQLTemplate');


test "Tag processor error case";
$sql_tmpl = DBIx::Custom::SQLTemplate->new;


eval{$sql_tmpl->create_query("{a }")};
like($@, qr/Tag '{a }' in SQL template is not exist/, "$test : tag_processor not exist");

$sql_tmpl->register_tag_processor({
    q => 'string'
});

eval{$sql_tmpl->create_query("{q}", {})};
like($@, qr/Tag processor 'q' must be code reference/, "$test : tag_processor not code ref");

$sql_tmpl->register_tag_processor({
   r => sub {} 
});

eval{$sql_tmpl->create_query("{r}")};
like($@, qr/\QTag processor 'r' must return (\E\$expand\Q, \E\$columns\Q)/, "$test : tag processor return noting");

$sql_tmpl->register_tag_processor({
   s => sub { return ("a", "")} 
});

eval{$sql_tmpl->create_query("{s}")};
like($@, qr/\QTag processor 's' must return (\E\$expand\Q, \E\$columns\Q)/, "$test : tag processor return not array columns");

$sql_tmpl->register_tag_processor(
    t => sub {return ("a", [])}
);

eval{$sql_tmpl->create_query("{t ???}")};
like($@, qr/Tag '{t }' arguments cannot contain '?'/, "$test : cannot contain '?' in tag argument");


test 'General error case';
$sql_tmpl = DBIx::Custom::SQLTemplate->new;
$sql_tmpl->register_tag_processor(
    a => sub {
        return ("? ? ?", [[],[]]);
    }
);
eval{$sql_tmpl->create_query("{a}")};
like($@, qr/Placeholder count in SQL created by tag processor 'a' must be same as key informations count/, "$test placeholder count is invalid");


test 'Default tag processor Error case';
eval{$sql_tmpl->create_query("{= }")};
like($@, qr/You must be pass key as argument to tag '{= }'/, "$test : basic '=' : key not exist");

eval{$sql_tmpl->create_query("{in }")};
like($@, qr/You must be pass key as first argument of tag '{in }'/, "$test : in : key not exist");

eval{$sql_tmpl->create_query("{in a}")};
like($@, qr/\QYou must be pass value count as second argument of tag '{in }'\E\n\QUsage: {in \E\$key\Q \E\$count\Q}/,
     "$test : in : key not exist");

eval{$sql_tmpl->create_query("{in a r}")};
like($@, qr/\QYou must be pass value count as second argument of tag '{in }'\E\n\QUsage: {in \E\$key\Q \E\$count\Q}/,
     "$test : in : key not exist");


test 'Clone';
$sql_tmpl = DBIx::Custom::SQLTemplate->new;
$sql_tmpl
  ->tag_start('[')
  ->tag_end(']')
  ->tag_syntax('syntax')
  ->tag_processors({a => 1, b => 2});

