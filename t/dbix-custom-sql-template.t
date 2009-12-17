use strict;
use warnings;

use Test::More 'no_plan';

use DBIx::Custom::SQL::Template;

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
my $clone;

test "Various template pattern";
$datas = [
    # Basic tests
    {   name            => 'placeholder basic',
        tmpl            => "a {?  k1} b {=  k2} {<> k3} {>  k4} {<  k5} {>= k6} {<= k7} {like k8}", ,
        sql_expected    => "a ? b k2 = ? k3 <> ? k4 > ? k5 < ? k6 >= ? k7 <= ? k8 like ?;",
        key_infos_expected   => [
            {original_key => 'k1', table => '', column => 'k1', access_keys => [['k1']]},
            {original_key => 'k2', table => '', column => 'k2', access_keys => [['k2']]},
            {original_key => 'k3', table => '', column => 'k3', access_keys => [['k3']]},
            {original_key => 'k4', table => '', column => 'k4', access_keys => [['k4']]},
            {original_key => 'k5', table => '', column => 'k5', access_keys => [['k5']]},
            {original_key => 'k6', table => '', column => 'k6', access_keys => [['k6']]},
            {original_key => 'k7', table => '', column => 'k7', access_keys => [['k7']]},
            {original_key => 'k8', table => '', column => 'k8', access_keys => [['k8']]},
        ],
    },
    {
        name            => 'placeholder in',
        tmpl            => "{in k1 3};",
        sql_expected    => "k1 in (?, ?, ?);",
        key_infos_expected   => [
            {original_key => 'k1', table => '', column => 'k1', access_keys => [['k1', [0]]]},
            {original_key => 'k1', table => '', column => 'k1', access_keys => [['k1', [1]]]},
            {original_key => 'k1', table => '', column => 'k1', access_keys => [['k1', [2]]]},
        ],
    },
    {
        name            => 'insert',
        tmpl            => "{insert k1 k2 k3}",
        sql_expected    => "(k1, k2, k3) values (?, ?, ?);",
        key_infos_expected   => [
            {original_key => 'k1', table => '', column => 'k1', access_keys => [['#insert', 'k1'], ['k1']]},
            {original_key => 'k2', table => '', column => 'k2', access_keys => [['#insert', 'k2'], ['k2']]},
            {original_key => 'k3', table => '', column => 'k3', access_keys => [['#insert', 'k3'], ['k3']]},
        ],
    },
    {
        name            => 'update',
        tmpl            => "{update k1 k2 k3}",
        sql_expected    => "set k1 = ?, k2 = ?, k3 = ?;",
        key_infos_expected   => [
            {original_key => 'k1', table => '', column => 'k1', access_keys => [['#update', 'k1'], ['k1']]},
            {original_key => 'k2', table => '', column => 'k2', access_keys => [['#update', 'k2'], ['k2']]},
            {original_key => 'k3', table => '', column => 'k3', access_keys => [['#update', 'k3'], ['k3']]},
        ],
    },
    
    # Table name
    {
        name            => 'placeholder with table name',
        tmpl            => "{= a.k1} {= a.k2}",
        sql_expected    => "a.k1 = ? a.k2 = ?;",
        key_infos_expected  => [
            {original_key => 'a.k1', table => 'a', column => 'k1', access_keys => [['a.k1'], ['a', 'k1']]},
            {original_key => 'a.k2', table => 'a', column => 'k2', access_keys => [['a.k2'], ['a', 'k2']]},
        ],
    },
    {   
        name            => 'placeholder in with table name',
        tmpl            => "{in a.k1 2} {in b.k2 2}",
        sql_expected    => "a.k1 in (?, ?) b.k2 in (?, ?);",
        key_infos_expected  => [
            {original_key => 'a.k1', table => 'a', column => 'k1', access_keys => [['a.k1', [0]], ['a', 'k1', [0]]]},
            {original_key => 'a.k1', table => 'a', column => 'k1', access_keys => [['a.k1', [1]], ['a', 'k1', [1]]]},
            {original_key => 'b.k2', table => 'b', column => 'k2', access_keys => [['b.k2', [0]], ['b', 'k2', [0]]]},
            {original_key => 'b.k2', table => 'b', column => 'k2', access_keys => [['b.k2', [1]], ['b', 'k2', [1]]]},
        ],
    },
    {
        name            => 'insert with table name',
        tmpl            => "{insert a.k1 b.k2}",
        sql_expected    => "(k1, k2) values (?, ?);",
        key_infos_expected  => [
            {original_key => 'a.k1', table => 'a', column => 'k1', access_keys => [['#insert', 'a.k1'], ['#insert', 'a', 'k1'], ['a.k1'], ['a', 'k1']]},
            {original_key => 'b.k2', table => 'b', column => 'k2', access_keys => [['#insert', 'b.k2'], ['#insert', 'b', 'k2'], ['b.k2'], ['b', 'k2']]},
        ],
    },
    {
        name            => 'update with table name',
        tmpl            => "{update a.k1 b.k2}",
        sql_expected    => "set k1 = ?, k2 = ?;",
        key_infos_expected  => [
            {original_key => 'a.k1', table => 'a', column => 'k1', access_keys => [['#update', 'a.k1'], ['#update', 'a', 'k1'], ['a.k1'], ['a', 'k1']]},
            {original_key => 'b.k2', table => 'b', column => 'k2', access_keys => [['#update', 'b.k2'], ['#update', 'b', 'k2'], ['b.k2'], ['b', 'k2']]},
        ],
    },
    {
        name            => 'not contain tag',
        tmpl            => "aaa",
        sql_expected    => "aaa;",
        key_infos_expected  => [],
    }
];

for (my $i = 0; $i < @$datas; $i++) {
    my $data = $datas->[$i];
    my $sql_tmpl = DBIx::Custom::SQL::Template->new;
    my $query = $sql_tmpl->create_query($data->{tmpl});
    is($query->{sql}, $data->{sql_expected}, "$test : $data->{name} : sql");
    is_deeply($query->{key_infos}, $data->{key_infos_expected}, "$test : $data->{name} : key_infos");
}


test 'Original tag processor';
$sql_tmpl = DBIx::Custom::SQL::Template->new;

$ret_val = $sql_tmpl->add_tag_processor(
    p => sub {
        my ($tag_name, $args) = @_;
        
        my $expand    = "$tag_name ? $args->[0] $args->[1]";
        my $key_infos = [2];
        return ($expand, $key_infos);
    }
);

$query = $sql_tmpl->create_query("{p a b}");
is($query->{sql}, "p ? a b;", "$test : add_tag_processor sql");
is_deeply($query->{key_infos}, [2], "$test : add_tag_processor key_infos");
isa_ok($ret_val, 'DBIx::Custom::SQL::Template');


test "Tag processor error case";
$sql_tmpl = DBIx::Custom::SQL::Template->new;


eval{$sql_tmpl->create_query("{a }")};
like($@, qr/Tag '{a }' in SQL template is not exist/, "$test : tag_processor not exist");

$sql_tmpl->add_tag_processor({
    q => 'string'
});

eval{$sql_tmpl->create_query("{q}", {})};
like($@, qr/Tag processor 'q' must be code reference/, "$test : tag_processor not code ref");

$sql_tmpl->add_tag_processor({
   r => sub {} 
});

eval{$sql_tmpl->create_query("{r}")};
like($@, qr/\QTag processor 'r' must return (\E\$expand\Q, \E\$key_infos\Q)/, "$test : tag processor return noting");

$sql_tmpl->add_tag_processor({
   s => sub { return ("a", "")} 
});

eval{$sql_tmpl->create_query("{s}")};
like($@, qr/\QTag processor 's' must return (\E\$expand\Q, \E\$key_infos\Q)/, "$test : tag processor return not array key_infos");

$sql_tmpl->add_tag_processor(
    t => sub {return ("a", [])}
);

eval{$sql_tmpl->create_query("{t ???}")};
like($@, qr/Tag '{t }' arguments cannot contain '?'/, "$test : cannot contain '?' in tag argument");


test 'General error case';
$sql_tmpl = DBIx::Custom::SQL::Template->new;
$sql_tmpl->add_tag_processor(
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
like($@, qr/\QYou must be pass placeholder count as second argument of tag '{in }'\E\n\QUsage: {in \E\$key\Q \E\$placeholder_count\Q}/,
     "$test : in : key not exist");

eval{$sql_tmpl->create_query("{in a r}")};
like($@, qr/\QYou must be pass placeholder count as second argument of tag '{in }'\E\n\QUsage: {in \E\$key\Q \E\$placeholder_count\Q}/,
     "$test : in : key not exist");


test 'Clone';
$sql_tmpl = DBIx::Custom::SQL::Template->new;
$sql_tmpl
  ->tag_start('[')
  ->tag_end(']')
  ->tag_syntax('syntax')
  ->tag_processors({a => 1, b => 2});

$clone = $sql_tmpl->clone;
is($clone->tag_start, $sql_tmpl->tag_start, "$test : tag_start");
is($clone->tag_end, $sql_tmpl->tag_end, "$test : tag_end");
is($clone->tag_syntax, $sql_tmpl->tag_syntax, "$test : tag_syntax");

is_deeply( scalar $clone->tag_processors, scalar $sql_tmpl->tag_processors,
          "$test : tag_processors deep clone");

isnt($clone->tag_processors, $sql_tmpl->tag_processors, 
     "$test : tag_processors reference not copy");

$sql_tmpl->tag_processors(undef);

$clone = $sql_tmpl->clone;
is_deeply(scalar $clone->tag_processors, {}, "$test tag_processor undef copy");



__END__



