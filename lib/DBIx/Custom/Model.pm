package DBIx::Custom::Model;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util qw/_subname _deprecate/;

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

has [qw/dbi table ctime mtime bind_type join primary_key/],
  columns => sub { [] };

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
delete delete_at delete_all select select_at count/;
for my $method (@methods) {
  
  my $code =
       qq/sub {/ .
       qq/my \$self = shift;/ .
       qq/\$self->dbi->$method(/ .
           qq/\@_ % 2 ? shift : (),/;

  
  my @attrs = qw/table type primary_key bind_type/;
  my @insert_attrs = qw/created_at updated_at ctime mtime/;
  my @update_attrs = qw/updated_at mtime/;
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
  croak $code if $@;
}

sub update_or_insert {
  my ($self, $param, %opt) = @_;
  
  croak "update_or_insert method need primary_key and id option "
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
  else { croak "selected row must be one " . _subname }
}

sub execute {
  my $self = shift;
  
  if ($ENV{DBIX_CUSTOM_DISABLE_MODEL_EXECUTE}) {
      $self->dbi->execute(@_);
  }
  else {
    _deprecate('0.24', "DBIx::Custom::Model execute method is DEPRECATED! " .
      "use DBIx::Custom execute method. " .
      "If you want to call DBIx::Custom execute method directory from model, " .
      "set \$ENV{DBIX_CUSTOM_DISABLE_MODEL_EXECUTE} to 1 " .
      "until DBIx::Custom::Model execute method is removed in the future." );
    
    return $self->dbi->execute(
      shift,
      shift,
      table => $self->table,
      bind_type => $self->bind_type,
      primary_key => $self->primary_key,
      type => $self->type,
      @_
    );    
  }
}

sub DESTROY { }

sub helper {
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
  for my $attr (@attrs) {
    croak qq{"$attr" is invalid attribute name } . _subname
      unless $self->can($attr);
  }
  
  # Cache
  for my $attr (qw/dbi table created_at updated_at ctime mtime bind_type join primary_key/) {
    $self->$attr;
    $self->{$attr} = undef unless exists $self->{$attr};
  }
  $self->columns;
  
  return $self;
}

# DEPRECATED!
has 'filter';
has 'name';
has 'type';
has 'created_at';
has 'updated_at'; 

# DEPRECATED!
sub method {
  _deprecate('0.24', "method method is DEPRECATED! use helper instead");
  return shift->helper(@_);
}

1;

=head1 NAME

DBIx::Custom::Model - Model object

=head1 SYNOPSIS

use DBIx::Custom::Model;

my $model = DBIx::Custom::Model->new(table => 'books');

=head1 ATTRIBUTES

=head2 dbi

  my $dbi = $model->dbi;
  $model = $model->dbi($dbi);

L<DBIx::Custom> object.

=head2 ctime

  my $ctime = $model->ctime;
  $model = $model->ctime('created_time');

Create timestamp column, this is passed to C<insert> or C<update> method.

=head2 join

  my $join = $model->join;
  $model = $model->join(
    ['left outer join company on book.company_id = company.id']
  );
  
Join clause, this value is passed to C<select> method.

=head2 primary_key

  my $primary_key = $model->primary_key;
  $model = $model->primary_key(['id', 'number']);

Primary key,this is passed to C<insert>, C<update>,
C<delete>, and C<select> method.

=head2 table

  my $model = $model->table;
  $model = $model->table('book');

Table name, this is passed to C<select> method.

=head2 bind_type

  my $type = $model->bind_type;
  $model = $model->bind_type(['image' => DBI::SQL_BLOB]);
  
Database data type, this is used as type optioon of C<insert>, 
C<update>, C<update_all>, C<delete>, C<delete_all>,
and C<select> method

=head2 mtime

  my $mtime = $model->mtime;
  $model = $model->mtime('modified_time');

Updated timestamp column, this is passed to C<update> method.

=head1 METHODS

L<DBIx::Custom::Model> inherits all methods from L<Object::Simple>,
and you can use all methods of L<DBIx::Custom> and L<DBI>
and implements the following new ones.

=head2 count

  my $count = $model->count;

Get rows count.

Options is same as C<select> method's ones.

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

=head2 helper

  $model->helper(
    update_or_insert => sub {
      my $self = shift;
      
      # ...
    },
    find_or_create   => sub {
      my $self = shift;
      
      # ...
    }
  );

Register helper. These helper is called directly from L<DBIx::Custom::Model> object.

  $model->update_or_insert;
  $model->find_or_create;

=head2 mycolumn

  my $column = $self->mycolumn;
  my $column = $self->mycolumn(book => ['author', 'title']);
  my $column = $self->mycolumn(['author', 'title']);

Create column clause for myself. The follwoing column clause is created.

  book.author as author,
  book.title as title

If table name is ommited, C<table> attribute of the model is used.
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

  $model->update_all(param => \%param);
  
Same as C<update_all> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=head2 update_or_insert

  $model->update_or_insert(...);
  
Same as C<update> of L<DBIx::Custom> except that
you don't have to specify options if you set attribute in model.

=cut
