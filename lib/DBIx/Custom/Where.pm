package DBIx::Custom::Where;
use Object::Simple -base;

use Carp 'confess';
use DBIx::Custom::Util '_subname';
use overload 'bool' => sub {1}, fallback => 1;
use overload '""' => sub { shift->to_string }, fallback => 1;

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

has 'dbi';
has 'param';
has clause => sub { [] };
has 'join';

sub new {
  my $self = shift->SUPER::new(@_);
  
  # Check attribute names
  my @attrs = keys %$self;
  for my $attr (@attrs) {
    confess qq{"$attr" is invalid attribute name (} . _subname . ")"
      unless $self->can($attr);
  }
  
  return $self;
}

sub to_string {
  my $self = shift;
  
  # Clause
  my $clause = $self->clause;
  $clause = ['and', $clause] unless ref $clause eq 'ARRAY';
  $clause->[0] = 'and' unless @$clause;
  
  # Parse
  my $where = [];
  my $count = {};
  my $c = $self->dbi->safety_character;
  $self->{_re} = $c eq 'a-zA-Z0-9_' ?
    qr/[^\\]:([$c\.]+)/so : qr/[^\\]:([$c\.]+)/s;
  
  $self->{_quote} = $self->dbi->_quote;
  $self->{_tag_parse} = exists $ENV{DBIX_CUSTOM_TAG_PARSE}
    ? $ENV{DBIX_CUSTOM_TAG_PARSE} : $self->dbi->{tag_parse};
  $self->_parse($clause, $where, $count, 'and');

  # Stringify
  unshift @$where, 'where' if @$where;
  return join(' ', @$where);
}
  
our %VALID_OPERATIONS = map { $_ => 1 } qw/and or/;
sub _parse {
  my ($self, $clause, $where, $count, $op, $info) = @_;
  
  # Array
  if (ref $clause eq 'ARRAY') {
    # Start
    push @$where, '(';
    
    # Operation
    my $op = $clause->[0] || '';
    confess qq{First argument must be "and" or "or" in where clause } .
        qq{"$op" is passed} . _subname . ")"
      unless $VALID_OPERATIONS{$op};
    
    my $pushed_array;
    # Parse internal clause
    for (my $i = 1; $i < @$clause; $i++) {
      my $pushed = $self->_parse($clause->[$i], $where, $count, $op);
      push @$where, $op if $pushed;
      $pushed_array = 1 if $pushed;
    }
    pop @$where if $where->[-1] eq $op;
    
    # Undo
    if ($where->[-1] eq '(') {
      pop @$where;
      pop @$where if ($where->[-1] || '') eq $op;
    }
    # End
    else { push @$where, ')' }
    
    return $pushed_array;
  }
  
  # String
  else {
    # Pushed
    my $pushed;
    
    # Column
    my $re = $self->{_re};
    
    my $column;
    my $sql = " " . $clause || '';
    $sql =~ s/([0-9]):/$1\\:/g;
    ($column) = $sql =~ /$re/;

    unless (defined $column) {
      push @$where, $clause;
      $pushed = 1;
      return $pushed;
    }
    
    # Column count up
    my $count = ++$count->{$column};
    
    # Push
    my $param = $self->{param};
    if (ref $param eq 'HASH') {
      if (exists $param->{$column}) {
        if (ref $param->{$column} eq 'ARRAY') {
          $pushed = 1 if exists $param->{$column}->[$count - 1]
            && ref $param->{$column}->[$count - 1] ne 'DBIx::Custom::NotExists'
        }
        elsif ($count == 1) { $pushed = 1 }
      }
      push @$where, $clause if $pushed;
    }
    elsif (!defined $param) {
      push @$where, $clause;
      $pushed = 1;
    }
    else {
      confess "Parameter must be hash reference or undfined value ("
          . _subname . ")"
    }
    return $pushed;
  }
  return;
}
1;

=head1 NAME

DBIx::Custom::Where - Where clause

=head1 SYNOPSYS
  
  # Create DBIx::Custom::Where object
  my $where = $dbi->where;
  
  # Clause
  $where->clause(['and', 'title like :title', 'price = :price']);
  $where->clause(['and', ':title{like}', ':price{=}']);
  
  # Stringify where clause
  my $where_clause = "$where";
  my $where_clause = $where->to_string;
    # -> where title like :title and price = :price
  
  # Only price condition
  $where->clause(['and', ':title{like}', ':price{=}']);
  $where->param({price => 1900});
    # -> where price = :price
  
  # Only title condition
  $where->clause(['and', ':title{like}', ':price{=}']);
  $where->param({title => 'Perl'});
    # -> where title like :title
  
  # Nothing
  $where->clause(['and', ':title{like}', ':price{=}']);
  $where->param({});
    # => Nothing
  
  # or condition
  $where->clause(['or', ':title{like}', ':price{=}']);
    # -> where title = :title or price like :price
  
  # More than one parameter
  $where->clause(['and', ':price{>}', ':price{<}']);
  $where->param({price => [1000, 2000]});
    # -> where price > :price and price < :price
  
  # Only first condition
  $where->clause(['and', ':price{>}', ':price{<}']);
  $where->param({price => [1000, $dbi->not_exists]});
    # -> where price > :price
  
  # Only second condition
  $where->clause(['and', ':price{>}', ':price{<}']);
  $where->param({price => [$dbi->not_exists, 2000]});
    # -> where price < :price
  
  # More complex condition
  $where->clause(
    [
      'and',
      ':price{=}',
      ['or', ':title{=}', ':title{=}', ':title{=}']
    ]
  );
    # -> pirce = :price and (title = :title or title = :title or tilte = :title)
  
  # Using Full-qualified column name
  $where->clause(['and', ':book.title{like}', ':book.price{=}']);
    # -> book.title like :book.title and book.price = :book.price

=head1 ATTRIBUTES

=head2 clause

  my $clause = $where->clause;
  $where = $where->clause(
    ['and',
      ':title{=}', 
      ['or', ':date{<}', ':date{>}']
    ]
  );

Where clause. Above one is expanded to the following SQL by to_string
If all parameter names is exists.

  where title = :title and ( date < :date or date > :date )

=head2 param

  my $param = $where->param;
  $where = $where->param({
    title => 'Perl',
    date => ['2010-11-11', '2011-03-05'],
  });

=head2 dbi

  my $dbi = $where->dbi;
  $where = $where->dbi($dbi);

L<DBIx::Custom> object.

=head2 join

  my $join = $where->join;
  $join = $where->join($join);

join information. This values is addd to select method C<join> option values.

  $where->join(['left join author on book.author = authro.id']);
  
=head1 METHODS

L<DBIx::Custom::Where> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 to_string

  $where->to_string;

Convert where clause to string.

double quote is override to execute C<to_string> method.

  my $string_where = "$where";

=cut
