# DEPRECATED!
package DBIx::Custom::QueryBuilder;

use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::Util qw/_subname _deprecate/;

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;
push @DBIx::Custom::Where::CARP_NOT, __PACKAGE__;

# DEPRECATED!
sub build_query {
  my ($self, $sql) = @_;

  my $query = $self->_parse_tag($sql);
  my $tag_count = delete $query->{tag_count};
  _deprecate('0.24', qq/Tag system such as {? name} is DEPRECATED! / .
      qq/use parameter system such as :name instead/)
    if $tag_count;
  my $query2 = $self->_parse_parameter($query->sql);
  $query->sql($query2->sql);
  for (my $i =0; $i < @{$query->columns}; $i++) {
    my $column = $query->columns->[$i];
    if ($column eq 'RESERVED_PARAMETER') {
      my $column2 = shift @{$query2->columns};
      croak ":name syntax is wrong"
        unless defined $column2;
      $query->columns->[$i] = $column2;
    }
  }
  return $query;
}

# DEPRECATED!
sub _parse_parameter {
  my ($self, $source) = @_;
  
  # Get and replace parameters
  my $sql = $source || '';
  my $columns = [];
  my $c = $self->dbi->safety_character;
  # Parameter regex
  $sql =~ s/([^:]):(\d+):([^:])/$1\\:$2\\:$3/g;
  my $re = qr/(^|.*?[^\\]):([$c\.]+)(?:\{(.*?)\})?(.*)/s;
  while ($sql =~ /$re/g) {
    push @$columns, $2;
    $sql = defined $3 ? "$1$2 $3 ?$4" : "$1?$4";
  }
  $sql =~ s/\\:/:/g;

  # Create query
  my $query = DBIx::Custom::Query->new(
    sql => $sql,
    columns => $columns
  );
  
  return $query;
}

# DEPRECATED
has 'dbi';

# DEPRECATED!
has tags => sub { {} };

# DEPRECATED!
sub register_tag {
  my $self = shift;
  
  _deprecate('0.24', "register_tag is DEPRECATED!");
  
  # Merge tag
  my $tags = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  $self->tags({%{$self->tags}, %$tags});
  
  return $self;
}

# DEPRECATED!
sub _parse_tag {
  my ($self, $source) = @_;
  # Source
  $source ||= '';
  # Tree
  my @tree;
  # Value
  my $value = '';
  # State
  my $state = 'text';
  # Before charactor
  my $before = '';
  # Position
  my $pos = 0;
  # Parse
  my $original = $source;
  my $tag_count = 0;
  while (defined(my $c = substr($source, $pos, 1))) {
    # Last
    last unless length $c;
    # Parameter
    if ($c eq ':' && (substr($source, $pos + 1, 1) || '') =~ /\w/) {
        push @tree, {type => 'param'};;
    }
    # State is text
    if ($state eq 'text') {
      # Tag start charactor
      if ($c eq '{') {
        # Escaped charactor
        if ($before eq "\\") {
          substr($value, -1, 1, '');
          $value .= $c;
        }
        # Tag start
        else {
          # Change state
          $state = 'tag';
          # Add text
          push @tree, {type => 'text', value => $value}
            if $value;
          # Clear
          $value = '';
        }
      }
      # Tag end charactor
      elsif ($c eq '}') {
        # Escaped charactor
        if ($before eq "\\") {
          substr($value, -1, 1, '');
          $value .= $c;
        }
        # Unexpected
        else {
          croak qq{Parsing error. unexpected "\}". }
            . qq{pos $pos of "$original" } . _subname
        }
      }
      # Normal charactor
      else { $value .= $c }
    }
    # State is tags
    else {
      # Tag start charactor
      if ($c eq '{') {
        # Escaped charactor
        if ($before eq "\\") {
          substr($value, -1, 1, '');
          $value .= $c;
        }
        # Unexpected
        else {
          croak qq{Parsing error. unexpected "\{". }
              . qq{pos $pos of "$original" } . _subname
        }
      }
      # Tag end charactor
      elsif ($c eq '}') {
        # Escaped charactor
        if ($before eq "\\") {
          substr($value, -1, 1, '');
          $value .= $c;
        }
        # Tag end
        else {
          # Change state
          $state = 'text';
          # Add tag
          my ($tag_name, @tag_args) = split /\s+/, $value;
          push @tree, {type => 'tag', tag_name => $tag_name, 
                       tag_args => \@tag_args};
          # Clear
          $value = '';
          # Countup
          $tag_count++;
        }
      }
      # Normal charactor
      else { $value .= $c }
    }
    # Save before charactor
    $before = $c;
    # increment position
    $pos++;
  }
  # Tag not finished
  croak qq{Tag not finished. "$original" } . _subname
    if $state eq 'tag';
  # Not contains tag
  return DBIx::Custom::Query->new(sql => $source, tag_count => $tag_count)
    if $tag_count == 0;
  # Add rest text
  push @tree, {type => 'text', value => $value}
    if $value;
  # SQL
  my $sql = '';
  # All Columns
  my $all_columns = [];
  # Tables
  my $tables = [];
  # Build SQL 
  for my $node (@tree) {
    # Text
    if ($node->{type} eq 'text') { $sql .= $node->{value} }
    # Parameter
    elsif ($node->{type} eq 'param') {
        push @$all_columns, 'RESERVED_PARAMETER';
    }
    # Tag
    else {
      # Tag name
      my $tag_name = $node->{tag_name};
      # Tag arguments
      my $tag_args = $node->{tag_args};
      # Table
      if ($tag_name eq 'table') {
        my $table = $tag_args->[0];
        push @$tables, $table;
        $sql .= $table;
        next;
      }
      # Get tag
      $self->dbi->{_tags} ||= {};
      my $tag = $self->tag_processors->{$tag_name}
                       || $self->dbi->{_tags}->{$tag_name};
      # Tag is not registered
      croak qq{Tag "$tag_name" is not registered } . _subname
        unless $tag;
      # Tag not sub reference
      croak qq{Tag "$tag_name" must be sub reference } . _subname
        unless ref $tag eq 'CODE';
      # Execute tag
      my $r = $tag->(@$tag_args);
      # Check tag return value
      croak qq{Tag "$tag_name" must return [STRING, ARRAY_REFERENCE] }
          . _subname
        unless ref $r eq 'ARRAY' && defined $r->[0] && ref $r->[1] eq 'ARRAY';
      # Part of SQL statement and colum names
      my ($part, $columns) = @$r;
      # Add columns
      push @$all_columns, @$columns;
      # Join part tag to SQL
      $sql .= $part;
    }
  }
  # Query
  my $query = DBIx::Custom::Query->new(
    sql => $sql,
    columns => $all_columns,
    tables => $tables,
    tag_count => $tag_count
  );
  return $query;
}

# DEPRECATED!
has tag_processors => sub { {} };

# DEPRECATED!
sub register_tag_processor {
  my $self = shift;
  _deprecate('0.24', "register_tag_processor is DEPRECATED!");
  # Merge tag
  my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
  $self->tag_processors({%{$self->tag_processors}, %{$tag_processors}});
  return $self;
}

1;

=head1 NAME

DBIx::Custom::QueryBuilder - DEPRECATED!

=cut
