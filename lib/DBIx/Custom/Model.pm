package DBIx::Custom::Model;
use Object::Simple -base;

use Carp 'confess';
use DBIx::Custom::Util qw/_subname _deprecate/;

has [qw/dbi table name ctime mtime bind_type join/];
has columns => sub { [] };

our $AUTOLOAD;

my @methods = qw(insert update update_all delete delete_all select count);
for my $method (@methods) {
  
  my $code =
       qq/sub {/ .
       qq/my \$self = shift;/ .
       qq/\$self->dbi->$method(/ .
           qq/\@_ % 2 ? shift : (),/;

  
  my @attrs = qw/table type primary_key bind_type/;
  my @insert_attrs = qw/ctime mtime/;
  my @update_attrs = qw/mtime/;
  my @select_attrs = qw/join/;
  if ($method eq 'insert') { push @attrs, @insert_attrs }
  elsif ($method eq 'update') { push @attrs, @update_attrs }
  elsif (index($method, 'select') != -1 || $method eq 'count') {
    push @attrs, @select_attrs
  }
  
  for my $attr (@attrs) {
    $code .= "exists \$self->{$attr} ? ($attr => \$self->{$attr}) : (),";
  }
  
  $code .= qq/\@_);/ .
       qq/}/;
  
  no strict 'refs';
  *{__PACKAGE__ . "::$method"} = eval $code;
  confess $code if $@;
}

# DEPRECATED
sub primary_key {
  if (@_ == 1) {
    return $_[0]{'primary_key'};
  }
  $_[0]{'primary_key'} = $_[1];
  $_[0];
};

# DEPRECATED
sub update_or_insert {
  my ($self, $param, %opt) = @_;

  _deprecate('0.39', "DBIx::Custom::Model::update_or_insert method is DEPRECATED!");

  confess "update_or_insert method need primary_key and id option "
    unless (defined $opt{id} || defined $self->{id})
        && (defined $opt{primary_key} || defined $self->{primary_key});
  
  my $statement_opt = $opt{option} || {};
  my $rows = $self->select(%opt, %{$statement_opt->{select} || {}})->all;
  if (@$rows == 0) {
    return $self->insert($param, %opt, %{$statement_opt->{insert} || {}});
  }
  elsif (@$rows == 1) {
    return $self->update($param, %opt, %{$statement_opt->{update} || {}});
  }
  else { confess "selected row must be one " . _subname }
}

# DEPRECATED
sub AUTOLOAD {
  my $self = shift;
  
  _deprecate('0.39', "DBIx::Custom::Model AUTOLOAD feature is DEPRECATED!");
  
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
    confess qq{Can't locate object method "$mname" via "$package" }
      . _subname;
  }
}
sub DESTROY { }

# DEPRECATED
sub helper {
  my $self = shift;
  
  _deprecate('0.39', "DBIx::Custom::Model::helper method is DEPRECATED!");
  
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
  for my $attr (@attrs) {
    confess qq{"$attr" is invalid attribute name } . _subname
      unless $self->can($attr);
  }
  
  # Cache
  for my $attr (qw/dbi table ctime mtime bind_type join primary_key/) {
    $self->$attr;
    $self->{$attr} = undef unless exists $self->{$attr};
  }
  $self->columns;
  
  return $self;
}

1;

=head1 NAME

DBIx::Custom::Model - Model object

=head1 SYNOPSIS

use DBIx::Custom::Model;

my $model = DBIx::Custom::Model->new(table => 'books');

=head1 ATTRIBUTES

=head2 name

  my $name = $model->name;
  $model = $model->name('book');

Model name.

=head2 table

  my $table = $model->table;
  $model = $model->table('book');

Table name, this is passed to C<insert>, C<update>, C<update_all>, C<delete>, C<delete_all>, C<select> method.

=head2 join

  my $join = $model->join;
  $model = $model->join(
    ['left outer join company on book.company_id = company.id']
  );
  
Join clause, this value is passed to C<select> method.

=head2 dbi

  my $dbi = $model->dbi;
  $model = $model->dbi($dbi);

L<DBIx::Custom> object.

=head2 bind_type

  my $type = $model->bind_type;
  $model = $model->bind_type(['image' => DBI::SQL_BLOB]);
  
Database data type, this is used as type option of C<insert>, 
C<update>, C<update_all>, C<delete>, C<delete_all>,
and C<select> method

=head2 mtime

  my $mtime = $model->mtime;
  $model = $model->mtime('modified_time');

Updated timestamp column, this is passed to C<update> method.

=head2 ctime

  my $ctime = $model->ctime;
  $model = $model->ctime('created_time');

Create timestamp column, this is passed to C<insert> or C<update> method.

=head2 primary_key

  my $primary_key = $model->primary_key;
  $model = $model->primary_key(['id', 'number']);

Primary key,this is passed to C<insert>, C<update>,
C<delete>, and C<select> method.

=head1 METHODS

L<DBIx::Custom::Model> inherits all methods from L<Object::Simple>,
and you can use all methods of L<DBIx::Custom> and L<DBI>
and implements the following new ones.

=head2 delete

  $model->delete(...);
  
Same as C<delete> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=head2 delete_all

  $model->delete_all(...);
  
Same as C<delete_all> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=head2 insert

  $model->insert(...);
  
Same as C<insert> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=head2 mycolumn

  my $column = $self->mycolumn;
  my $column = $self->mycolumn(book => ['author', 'title']);
  my $column = $self->mycolumn(['author', 'title']);

Create column clause for myself. The following column clause is created.

  book.author as author,
  book.title as title

If table name is omitted, C<table> attribute of the model is used.
If column names is omitted, C<columns> attribute of the model is used.

=head2 new

  my $model = DBIx::Custom::Model->new;

Create a L<DBIx::Custom::Model> object.

=head2 select

  $model->select(...);
  
Same as C<select> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=head2 update

  $model->update(...);
  
Same as C<update> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=head2 update_all

  $model->update_all(\%param);
  
Same as C<update_all> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=cut
