use Test::More 'no_plan';
use strict;
use warnings;

use DBI::Custom;
use Scalar::Util qw/blessed/;

# user password database
our ($U, $P, $D) = connect_info();


{
    my $dbi = DBI::Custom->new(
        connect_info => {
            user => 'a',
            password => 'b',
            data_source => 'c',
            options => {d => 1, e => 2}
        },
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        dbh => 'e',
    );
    
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}}                       ,filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g', dbh => 'e'}, 'new');
    
    isa_ok($dbi, 'DBI::Custom');
}

{
    package DBI::Custom::T1;
    use base 'DBI::Custom';
    
    __PACKAGE__->initialize_model(sub {
        my $model = shift;
        
        $model
          ->connect_info(
            user => 'a',
            password => 'b',
            data_source => 'c',
            options => {d => 1, e => 2}
          )
          ->filters(
            f => 3
          )
          ->bind_filter('f')
          ->fetch_filter('g')
          ->dbh('e')
    });
}
{
    my $dbi = DBI::Custom::T1->new(
        connect_info => {
            user => 'ao',
            password => 'bo',
            data_source => 'co',
            options => {do => 10, eo => 20}
        },
        filters => {
            fo => 30,
        },
        bind_filter => 'fo',
        fetch_filter => 'go',
    );
    
    is_deeply($dbi,{connect_info => {user => 'ao', password => 'bo', data_source => 'co', options => {do => 10, eo => 20}}                       ,filters => {fo => 30}, bind_filter => 'fo', fetch_filter => 'go'}, 'new arguments');
    
    isa_ok($dbi, 'DBI::Custom::T1');
}

{
    my $dbi = DBI::Custom::T1->new;
    
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}}                       ,filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g'}, 'new custom class');
    
    isa_ok($dbi, 'DBI::Custom::T1');
    
}

{
    package DBI::Custom::T1_2;
    use base 'DBI::Custom::T1';
}

{
    my $dbi = DBI::Custom::T1_2->new;
    
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}}                       ,filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g'}, 'new custom class inherit');
    
    isa_ok($dbi, 'DBI::Custom::T1_2');
}

{
    package DBI::Custom::T1_3;
    use base 'DBI::Custom::T1';
    
    __PACKAGE__->initialize_model(sub {
        my $model = shift;
        
        $model
          ->connect_info(
            user => 'ao',
            password => 'bo',
            data_source => 'co',
            options => {do => 10, eo => 20}
          )
          ->filters(
            fo => 30
          )
          ->bind_filter('fo')
          ->fetch_filter('go')
          ->dbh('eo')
    });
    
}

{
    my $dbi = DBI::Custom::T1_3->new;
    
    is_deeply($dbi,{connect_info => {user => 'ao', password => 'bo', data_source => 'co', options => {do => 10, eo => 20}}                       ,filters => {fo => 30}, bind_filter => 'fo', fetch_filter => 'go'}, 'new custom class');
    
    isa_ok($dbi, 'DBI::Custom::T1_3');
}

{
    my $dbi = DBI::Custom::T1_3->new(
        connect_info => {
            user => 'a',
            password => 'b',
            data_source => 'c',
            options => {d => 1, e => 2}
        },
        filters => {
            f => 3,
        },
        bind_filter => 'f',
        fetch_filter => 'g',
        dbh => 'e',
    );
    
    is_deeply($dbi,{connect_info => {user => 'a', password => 'b', data_source => 'c', options => {d => 1, e => 2}}                       ,filters => {f => 3}, bind_filter => 'f', fetch_filter => 'g', dbh => 'e'}, 'new');
    
    isa_ok($dbi, 'DBI::Custom');
}

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

{
    my $dbi = DBI::Custom->new(
        connect_info => {
            no_exist => 1,
        }
    );
    eval{$dbi->connect};
    
    like($@, qr/connect_info 'no_exist' is invald/, 'no exist');
}

sub connect_info {
    my $file = 'password.tmp';
    open my $fh, '<', $file
      or return;
    
    my ($user, $password, $database) = split(/\s/, (<$fh>)[0]);
    
    close $fh;
    
    return ($user, $password, $database);
}
