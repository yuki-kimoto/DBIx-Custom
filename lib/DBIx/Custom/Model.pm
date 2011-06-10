package DBIx::Custom::Model;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Util '_subname';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

__PACKAGE__->attr(
    ['dbi', 'name', 'table', 'view'],
    table_alias => sub { {} },
    columns => sub { [] },
    filter => sub { [] },
    join => sub { [] },
    type => sub { [] },
    primary_key => sub { [] }
);

our $AUTOLOAD;

sub AUTOLOAD {
    my $self = shift;

    # Method name
    my ($package, $mname) = $AUTOLOAD =~ /^([\w\:]+)\:\:(\w+)$/;

    # Method
    $self->{_methods} ||= {};
    if (my $method = $self->{_methods}->{$mname}) {
        return $self->$method(@_)
    }
    elsif (my $dbi_method = $self->dbi->can($mname)) {
        $self->dbi->$dbi_method(@_);
    }
    elsif ($self->{dbh} && (my $dbh_method = $self->dbh->can($mname))) {
        $self->dbi->dbh->$dbh_method(@_);
    }
    else {
        croak qq{Can't locate object method "$mname" via "$package" }
            . _subname;
    }
}

my @methods = qw/insert insert_at update update_at update_all
                 delete delete_at delete_all select select_at/;
foreach my $method (@methods) {

    my $code = sub {
        my $self = shift;

        my @args = (
            table => $self->table,
            type => $self->type,
            primary_key => $self->primary_key
        );
        push @args, (join => $self->join) if $method =~ /^select/;
        unshift @args, shift if @_ % 2;
        
        $self->dbi->$method(@args, @_);
    };
    
    no strict 'refs';
    my $class = __PACKAGE__;
    *{"${class}::$method"} = $code;
}

sub column {
    my ($self, $table, $columns) = @_;
    
    $self->{_table_alias} ||= {};
    my $dist;
    $dist = $self->dbi->{_table_alias}{$table}
          ? $self->dbi->{_table_alias}{$table}
          : $table;
    
    $self->dbi->{_model_from} ||= {};
    my $model = $self->dbi->{_model_from}->{$dist};
    
    $columns ||= $self->model($model)->columns;
    
    return $self->dbi->column($table, $columns);
}

sub col {
    my ($self, $table, $columns) = @_;
    
    $self->{_table_alias} ||= {};
    my $dist;
    $dist = $self->dbi->{_table_alias}{$table}
          ? $self->dbi->{_table_alias}{$table}
          : $table;
    
    $self->dbi->{_model_from} ||= {};
    my $model = $self->dbi->{_model_from}->{$dist};
    
    $columns ||= $self->model($model)->columns;
    
    return $self->dbi->col($table, $columns);
}

sub DESTROY { }

sub method {
    my $self = shift;
    
    # Merge
    my $methods = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->{_methods} = {%{$self->{_methods} || {}}, %$methods};
    
    return $self;
}

sub mycolumn {
    my $self = shift;
    my $table = shift unless ref $_[0];
    my $columns = shift;
    
    $table ||= $self->table || '';
    $columns ||= $self->columns;
    
    return $self->dbi->mycolumn($table, $columns);
}

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attribute names
    my @attrs = keys %$self;
    foreach my $attr (@attrs) {
        croak qq{"$attr" is invalid attribute name } . _subname
          unless $self->can($attr);
    }
    
    return $self;
}

1;

=head1 NAME

DBIx::Custom::Model - Model

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

This filter is applied when L<DBIx::Custom>'s C<include_model()> is called.

=head2 C<name>

    my $name = $model->name;
    $model   = $model->name('book');

Model name.

=head2 C<join>

    my $join = $model->join;
    $model   = $model->join(
        ['left outer join company on book.company_id = company.id']
    );
    
Join clause, this is used as C<select()>'s C<join> option.

=head2 C<table>

    my $table = $model->table;
    $model    = $model->table('book');

Table name, this is used as C<select()> C<table> option.
Generally, this is automatically set from class name.

=head2 C<table_alias> EXPERIMENTAL

    my $table_alias = $model->table_alias;
    $model = $model->table_alias(user1 => 'user', user2 => 'user');

Table alias. If you define table alias,
same filter as the table is avaliable
, and can write $dbi->column('user1') to get all columns.

=head2 C<primary_key>

    my $primary_key = $model->primary_key;
    $model          = $model->primary_key(['id', 'number']);

Foreign key, this is used as C<primary_key> of C<insert_at>,C<update_at()>,
C<delete_at()>,C<select_at()>.

=head2 C<type>

    my $type = $model->type;
    $model   = $model->type(['image' => DBI::SQL_BLOB]);
    
Database data type, this is used as type optioon of C<insert()>, C<insert_at()>,
C<update()>, C<update_at()>, C<update_all>, C<delete()>, C<delete_all()>,
C<select()>, C<select_at()>

=head2 C<view>

    my $view = $model->view;
    $model   = $model->view('select id, DATE(issue_datetime) as date from book');

View. This view is registered by C<view()> of L<DBIx::Custom> when
model is included by C<include_model>.

=head1 METHODS

L<DBIx::Custom> inherits all methods from L<Object::Simple>,
and you can use all methods of the object set to C<dbi>.
and implements the following new ones.

=head2 C<column> EXPERIMETNAL

    my $column = $model->column(book => ['author', 'title']);
    my $column = $model->column('book');

Create column clause. The follwoing column clause is created.

    book.author as book__author,
    book.title as book__title

If column names is omitted, C<columns> attribute of the model is used.

=head2 C<col> EXPERIMETNAL

    my $column = $model->col(book => ['author', 'title']);
    my $column = $model->col('book');

Create column clause. The follwoing column clause is created.

    book.author as "book.author",
    book.title as "book.title"

If column names is omitted, C<columns> attribute of the model is used.

=head2 C<delete>

    $table->delete(...);
    
Same as C<delete()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<delete_all>

    $table->delete_all(...);
    
Same as C<delete_all()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<insert>

    $table->insert(...);
    
Same as C<insert()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<method>

    $model->method(
        update_or_insert => sub {
            my $self = shift;
            
            # ...
        },
        find_or_create   => sub {
            my $self = shift;
            
            # ...
    );

Register method. These method is called directly from L<DBIx::Custom::Model> object.

    $model->update_or_insert;
    $model->find_or_create;

=head2 C<mycolumn>

    my $column = $self->mycolumn;
    my $column = $self->mycolumn(book => ['author', 'title']);
    my $column = $self->mycolumn(['author', 'title']);

Create column clause for myself. The follwoing column clause is created.

    book.author as author,
    book.title as title

If table name is ommited, C<table> attribute of the model is used.
If column names is omitted, C<columns> attribute of the model is used.

=head2 C<new>

    my $table = DBIx::Custom::Table->new;

Create a L<DBIx::Custom::Table> object.

=head2 C<select>

    $table->select(...);
    
Same as C<select()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<update>

    $table->update(...);
    
Same as C<update()> of L<DBIx::Custom> except that
you don't have to specify C<table> option.

=head2 C<update_all>

    $table->update_all(param => \%param);
    
Same as C<update_all()> of L<DBIx::Custom> except that
you don't have to specify table name.

=head2 C<update_at> DEPRECATED!

    $table->update_at(...);
    
Same as C<update_at()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.

=head2 C<select_at> DEPRECATED!

    $table->select_at(...);
    
Same as C<select_at()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.

=head2 C<insert_at> DEPRECATED!

    $table->insert_at(...);
    
Same as C<insert_at()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.

=head2 C<delete_at> DEPRECATED!

    $table->delete_at(...);
    
Same as C<delete()> of L<DBIx::Custom> except that
you don't have to specify C<table> and C<primary_key> option.
