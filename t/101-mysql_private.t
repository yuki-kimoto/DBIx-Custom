use Test::More;
use strict;
use warnings;

# user password database
our ($USER, $PASSWORD, $DATABASE) = connect_info();

plan skip_all => 'private MySQL test' unless $USER;

plan 'no_plan';

use DBI::Custom;
use Scalar::Util 'blessed';
{
    my $dbi = DBI::Custom->new(
        user => $USER,
        password => $PASSWORD,
        data_source => "dbi:mysql:dbname=$DATABASE"
    );
    $dbi->connect;
    
    ok(blessed $dbi->dbh);
    can_ok($dbi->dbh, qw/prepare/);
}

sub connect_info {
    my $file = 'password.tmp';
    open my $fh, '<', $file
      or return;
    
    my ($user, $password, $database) = split(/\s/, (<$fh>)[0]);
    
    close $fh;
    
    return ($user, $password, $database);
}
