use Test::More;
use strict;
use warnings;
use DBI;

BEGIN {
    eval { require DBD::SQLite; 1 }
        or plan skip_all => 'DBD::SQLite required';
    eval { DBD::SQLite->VERSION >= 1 }
        or plan skip_all => 'DBD::SQLite >= 1.00 required';

    plan 'no_plan';
    use_ok('DBI::Custom');
}

package Test::DBI::Custom;
use Object::Simple;

sub dbi : Attr {}

sub new {
    my $self = shift->SUPER::new;
    my $dbi = DBI::Custom->new->connect_info(data_source => 'dbi:SQLite:dbname=:memory:');
    
    $dbi->connect;
    $self->dbi($dbi);
    return $self;
}

sub create_table {
    my ($self, $create_table) = @_;
    $self->dbi->query_raw_sql($create_table);
    return $self;
}

sub create_table1 {
    my $self = shift;
    $self->create_table("create table t1 (k1 char(255), k2 char(255), k3 char(255), k4 char(255), k5 char(255));");
    return $self;
}

sub insert {
    my ($self, @values_list) = @_;
    my $table = ref $values_list[0] ? '' : shift;
    $table ||= 't1';
    
    foreach my $values (@values_list) {
        my $sql = $self->dbi->query(
            "insert into $table {insert_values}", {insert_values => $values}
        );
    }
    return $self;
}

sub test {
    my ($self, $code) = @_;
    $code->($self->dbi);
}

Object::Simple->build_class;

package main;
my $t = Test::DBI::Custom->new;

$t->new->create_table1->insert({k1 => 1, k2 => 2}, {k1 => 3, k2 => 4})->test(sub {
    my $dbi = shift;
    
    my $r;     # resultset
    my @rows;
    my $rows;
    
    #----------
    $r = $dbi->query("select k1, k2 from t1");
    
    @rows = ();
    while (my $row = $r->fetch) {
        push @rows, [@$row];
    }
    is_deeply(\@rows, [[1, 2], [3, 4]], 'fetch');
    
    
    #----------
    $r = $dbi->query("select k1, k2 from t1");
    
    @rows = ();
    while (my @row = $r->fetch) {
        push @rows, [@row];
    }
    is_deeply(\@rows, [[1, 2], [3, 4]], 'fetch list context');
    
    
    #-----------
    $r = $dbi->query("select k1, k2 from t1;");
    
    @rows = ();
    while (my $row = $r->fetch_hash) {
        push @rows, {%$row};
    }
    is_deeply(\@rows, [{k1 => 1, k2 => 2}, {k1 => 3, k2 => 4}], 'fetch_hash');
    
    
    #-----------
    $r = $dbi->query("select k1, k2 from t1;");
    
    @rows = ();
    while (my %row = $r->fetch_hash) {
        push @rows, {%row};
    }
    is_deeply(\@rows, [{k1 => 1, k2 => 2}, {k1 => 3, k2 => 4}], 'fetch hash list context');
    
    
    #-----------
    $r = $dbi->query("select k1, k2 from t1");
    
    $rows = $r->fetch_all;
    is_deeply($rows, [[1, 2], [3, 4]], 'fetch_all');
    
    
    #------------
    $r = $dbi->query("select k1, k2 from t1");
    
    @rows = $r->fetch_all;
    is_deeply(\@rows, [[1, 2], [3, 4]], 'fetch_all list context');
    
    
    #------------
    $r = $dbi->query("select k1, k2 from t1");
    
    @rows = $r->fetch_all_hash;
    is_deeply($rows, [[1, 2], [3, 4]], 'fetch_all_hash');
    
    
    #-------------
    $r = $dbi->query("select k1, k2 from t1");
    
    @rows = $r->fetch_all;
    is_deeply(\@rows, [[1, 2], [3, 4]], 'fetch_all_hash list context');
    
    
    #---------------------------------------------------------------------
    $dbi->fetch_filter(sub {
        my ($key, $value) = @_;
        if ($key eq 'k1' && $value == 1 ) {
            return $value * 3;
        }
        return $value;
    });
    
    #-----------------------------------
    $r = $dbi->query("select k1, k2 from t1");
    
    $rows = $r->fetch_all;
    
    is_deeply($rows, [[3, 2], [3, 4]], 'fetch_filter array');
    
    
    #----------------------------------
    $r = $dbi->query("select k1, k2 from t1");
    
    $rows = $r->fetch_all_hash;
    
    is_deeply($rows, [{k1 => 3, k2 => 2}, {k1 => 3, k2 => 4}], 'fetch_filter hash');

});

