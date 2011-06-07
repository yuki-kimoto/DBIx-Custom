use Test::More;
use strict;
use warnings;

$SIG{__WARN__} = sub { warn $_[0] unless $_[0] =~ /DEPRECATED/};

# user password database
our ($USER, $PASSWORD, $DATABASE) = connect_info();

plan skip_all => 'private MySQL test' unless $USER;

plan 'no_plan';

use DBIx::Custom;
use Scalar::Util 'blessed';
{
    my $dbi = DBIx::Custom->connect(
        user => $USER,
        password => $PASSWORD,
        dsn => "dbi:mysql:dbname=$DATABASE"
    );
    $dbi->connect;
    
    ok(blessed $dbi->dbh);
    can_ok($dbi->dbh, qw/prepare/);
    ok($dbi->dbh->{AutoCommit});
    ok(!$dbi->dbh->{mysql_enable_utf8});
}

{
    my $dbi = DBIx::Custom->connect(
        user => $USER,
        password => $PASSWORD,
        dsn => "dbi:mysql:dbname=$DATABASE",
        dbi_options => {AutoCommit => 0, mysql_enable_utf8 => 1}
    );
    $dbi->connect;
    ok(!$dbi->dbh->{AutoCommit});
    #ok($dbi->dbh->{mysql_enable_utf8});
}

sub connect_info {
    my $file = 'password.tmp';
    open my $fh, '<', $file
      or return;
    
    my ($user, $password, $database) = split(/\s/, (<$fh>)[0]);
    
    close $fh;
    
    return ($user, $password, $database);
}
