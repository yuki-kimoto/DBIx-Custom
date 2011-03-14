package DBIx::Custom::Model;

use strict;
use warnings;

use base 'DBIx::Custom';

use Carp 'croak';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

__PACKAGE__->attr(
    ['dbi', 'name', 'table', 'view'],
    columns => sub { [] },
    filter => sub { [] },
    join => sub { [] },
    primary_key => sub { [] }
);

sub column_clause {
    my $self = shift;
    
    my $args = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    
    my $table   = $self->table;
    my $columns = $self->columns;
    my $add     = $args->{add} || [];
    my $remove  = $args->{remove} || [];
    my %remove  = map {$_ => 1} @$remove;
    my $prefix  = $args->{prefix} || '';
    
    my @column;
    foreach my $column (@$columns) {
        push @column, "$table.$column as $prefix$column"
          unless $remove{$column};
    }
    
    foreach my $column (@$add) {
        push @column, $column;
    }
    
    return join (', ', @column);
}

sub delete {
    my $self = shift;
    $self->dbi->delete(table => $self->table, @_);
}

sub delete_all {
    my $self = shift;
    $self->dbi->delete_all(table => $self->table, @_);
}

sub delete_at {
    my $self = shift;
    
    return $self->dbi->delete_at(
        table => $self->table,
        primary_key => $self->primary_key,
        @_
    );
}

sub DESTROY { }

sub insert {
    my $self = shift;
    $self->dbi->insert(table => $self->table, @_);
}

sub insert_at {
    my $self = shift;
    
    return $self->dbi->insert_at(
        table => $self->table,
        primary_key => $self->primary_key,
        @_
    );
}

sub select {
    my $self = shift;
    $self->dbi->select(
        table => $self->table,
        join => $self->join,
        @_
    );
}

sub select_at {
    my $self = shift;
    
    return $self->dbi->select_at(
        table => $self->table,
        primary_key => $self->primary_key,
        join => $self->join,
        @_
    );
}

sub update {
    my $self = shift;
    $self->dbi->update(table => $self->table, @_)
}

sub update_all {
    my $self = shift;
    $self->dbi->update_all(table => $self->table, @_);
}


sub update_at {
    my $self = shift;
    
    return $self->dbi->update_at(
        table => $self->table,
        primary_key => $self->primary_key,
        @_
    );
}

1;

=head1 NAME

DBIx::Custom::Model - Model EXPERIMENTAL

=head1 SYNOPSIS

use DBIx::Custom::Table;

my $table = DBIx::Custom::Model->new(table => 'books');

=head1 ATTRIBUTES

=head2 C<dbi>

    my $dbi = $model->dbi;
    $model  = $model->dbi($dbi);

L<DBIx::Custom> object.

=head2 C<filter>

    my $dbi = $model->filter
    $model  = $model->filter({out => 'tp_to_date', in => 'date_to_tp'});

This filter is applied when L<DBIx::Custom> C<include_model()> is called.

=head2 C<name>

    my $name = $model->name;
    $model   = $model->name('book');

Model name.

=head2 C<join>

    my $join = $model->join;
    $model   = $model->join(
        ['left outer join company on book.company_id = company.id']
    );
    
Default join clause. This is used by C<select()>.

=head2 C<table>

    my $table = $model->table;
    $model    = $model->table('book');

Table name. Model name and table name is different.
Table name is real table name in database.

=head2 C<primary_key>

    my $primary_key = $model->primary_key;
    $model          = $model->primary_key(['id', 'number']);

Foreign key. This is used by C<insert_at>,C<update_at()>,
C<delete_at()>,C<select_at()>.

=head2 C<view>

    my $view = $model->view;
    $model   = $model->view('select id, DATE(issue_datetime) as date from book');

View. This view is registered by C<view()> of L<DBIx::Custom> when
model is included by C<include_model>.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>,
and you can use all methods of the object set to C<dbi>.
and implements the following new ones.

=head2 C<column_clause()>

To create column clause automatically, use C<column_clause()>.
Valude of C<table> and C<columns> is used.

    my $column_clause = $model->column_clause;

If C<table> is 'book'ÅAC<column> is ['id', 'name'],
the following clause is created.

    book.id as id, book.name as name

These column name is for removing column name ambiguities.

If you remove some columns, use C<remove> option.

    my $column_clause = $model->column_clause(remove => ['id']);

If you add some column, use C<add> option.

    my $column_clause = $model->column_clause(add => ['company.id as company__id']);

If you add column name prefix, use C<prefix> option

    my $column_clause = $model->column_clause(prefix => 'book__');

The following clause is created.

    book.id as book__id, book.name as book__name

=head2 C<delete>

    $table->delete(...);
    
Same as C<delete()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<delete_all>

    $table->delete_all(...);
    
Same as C<delete_all()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<delete_at>

    $table->delete_at(...);
    
Same as C<delete()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.

=head2 C<insert>

    $table->insert(...);
    
Same as C<insert()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<insert>

    $table->insert_at(...);
    
Same as C<insert_at()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.

=head2 C<new>

    my $table = DBIx::Custom::Table->new;

Create a L<DBIx::Custom::Table> object.

=head2 C<select>

    $table->select(...);
    
Same as C<select()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<select_at>

    $table->select_at(...);
    
Same as C<select_at()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.

=head2 C<update>

    $table->update(...);
    
Same as C<update()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<update_all>

    $table->update_all(param => \%param);
    
Same as C<update_all()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<update_at>

    $table->update_at(...);
    
Same as C<update_at()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.
