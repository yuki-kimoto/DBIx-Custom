use Test::More;

eval {require DBIx::TransactionManager; 1}
  or plan skip_all => 'required DBIx::TransactionManager';

plan 'no_plan';

use DBIx::Custom;

# Function for test name
sub test { "# $_[0]\n" }

# Constant varialbes for test
my $CREATE_TABLE = {
    0 => 'create table table1 (key1 char(255), key2 char(255));',
};

my $NEW_ARGS = {
    0 => {data_source => 'dbi:SQLite:dbname=:memory:'}
};

# Variables
my $dbi;
my $result;
my $txn;

test 'transaction';
$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});
{
    my $txn = $dbi->txn_scope;
    $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    $dbi->insert(table => 'table1', param => {key1 => 2, key2 => 3});
    $txn->commit;
}
$result = $dbi->select(table => 'table1');
is_deeply(scalar $result->fetch_hash_all, [{key1 => 1, key2 => 2}, {key1 => 2, key2 => 3}],
          "commit");

$dbi = DBIx::Custom->connect($NEW_ARGS->{0});
$dbi->execute($CREATE_TABLE->{0});

{
    local $SIG{__WARN__} = sub {};
    {
        my $txn = $dbi->txn_scope;
        $dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
    }
}
$result = $dbi->select(table => 'table1');
ok(! $result->fetch_first, "rollback");

