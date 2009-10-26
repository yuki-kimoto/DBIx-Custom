use Test::More;
use strict;
use warnings;

# user password database
our ($U, $P, $D) = connect_info();

plan skip_all => 'private MySQL test' unless $U;

plan 'no_plan';

use DBI::Custom;
use Scalar::Util 'blessed';
{
    my $dbi = DBI::Custom->new(
        connect_info => {
            user => $U,
            password => $P,
            data_source => "dbi:mysql:$D"
        }
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
