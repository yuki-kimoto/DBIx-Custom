package DBIx::Custom::Query;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util '_subname';

has [qw/sth statement/],
    sql => '',
    columns => sub { [] };

1;

=head1 NAME

DBIx::Custom::Query - Query

=head1 SYNOPSIS
    
    my $query = DBIx::Custom::Query->new;
    my $sth = $query->sth;
    my $sql = $query->sql;
    my $columns = $query->columns;
    
=head1 ATTRIBUTES

=head2 C<columns>

    my $columns = $query->columns;
    $query      = $query->columns(['auhtor', 'title']);

Column names.

=head2 C<sql>

    my $sql = $query->sql;
    $query  = $query->sql('select * from books where author = ?;');

SQL statement.

=head2 C<sth>

    my $sth = $query->sth;
    $query  = $query->sth($sth);

Statement handle of L<DBI>

=head1 METHODS

L<DBIx::Custom::Query> inherits all methods from L<Object::Simple>.

=cut
