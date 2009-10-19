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
    
    $dbi->fetch_filter(sub {
        my ($key, $value) = @_;
        if ($key eq 'k1' && $value == 1 ) {
            return $value * 3;
        }
        return $value;
    });
    
    my $result = $dbi->query("select k1, k2 from t1");
    
    my $row = $result->fetch;
    my @values = @$row;
    $result->finish;
    
    is_deeply(\@values, [3, 2]);
});

