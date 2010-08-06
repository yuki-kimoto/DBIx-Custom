package DBIx::Custom::QueryBuilder;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder::TagProcessors;

__PACKAGE__->dual_attr('tag_processors', default => sub { {} }, inherit => 'hash_copy');
__PACKAGE__->register_tag_processor(
    '?'      => \&DBIx::Custom::QueryBuilder::TagProcessors::placeholder,
    '='      => \&DBIx::Custom::QueryBuilder::TagProcessors::equal,
    '<>'     => \&DBIx::Custom::QueryBuilder::TagProcessors::not_equal,
    '>'      => \&DBIx::Custom::QueryBuilder::TagProcessors::greater_than,
    '<'      => \&DBIx::Custom::QueryBuilder::TagProcessors::lower_than,
    '>='     => \&DBIx::Custom::QueryBuilder::TagProcessors::greater_than_equal,
    '<='     => \&DBIx::Custom::QueryBuilder::TagProcessors::lower_than_equal,
    'like'   => \&DBIx::Custom::QueryBuilder::TagProcessors::like,
    'in'     => \&DBIx::Custom::QueryBuilder::TagProcessors::in,
    'insert' => \&DBIx::Custom::QueryBuilder::TagProcessors::insert,
    'update' => \&DBIx::Custom::QueryBuilder::TagProcessors::update
);

__PACKAGE__->attr(tag_start => '{');
__PACKAGE__->attr(tag_end   => '}');

sub register_tag_processor {
    my $self = shift;
    
    # Merge tag processor
    my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->tag_processors({%{$self->tag_processors}, %{$tag_processors}});
    
    return $self;
}

sub build_query {
    my ($self, $source)  = @_;
    
    # Parse
    my $tree = $self->_parse($source);
    
    # Build query
    my $query = $self->_build_query($tree);
    
    return $query;
}

sub _parse {
    my ($self, $source) = @_;
    
    # Source
    $source ||= '';
    
    # Tree
    my $tree = [];
    
    # Start tag
    my $tag_start = quotemeta $self->tag_start;
    croak qq{tag_start must be a charactor}
      if !$tag_start || length $tag_start == 1;
    
    # End tag
    my $tag_end   = quotemeta $self->tag_end;
    croak qq{tag_end must be a charactor}
      if !$tag_end || length $tag_end == 1;
    
    # Tokenize
    my $state = 'text';
    
    # Save original source
    my $original = $source;
    
    # Parse
    while ($source =~ s/([^$tag_start]*?)$tag_start([^$tag_end].*?)$tag_end//sm) {
        my $text = $1;
        my $tag  = $2;
        
        # Parse tree
        push @$tree, {type => 'text', tag_args => [$text]} if $text;
        
        if ($tag) {
            # Get tag name and arguments
            my ($tag_name, @tag_args) = split /\s+/, $tag;
            
            # Tag processor not registerd
            croak qq{Tag "$tag" in "$original" is not registerd}
               unless $self->tag_processors->{$tag_name};
            
            # Check tag arguments
            foreach my $tag_arg (@tag_args) {
            
                # Cannot cantain placehosder '?'
                croak qq{Tag cannot contains "?"}
                  if $tag_arg =~ /\?/;
            }
            
            # Add tag to parsing tree
            push @$tree, {type => 'tag', tag_name => $tag_name,
                          tag_args => [@tag_args]};
        }
    }
    
    # Add text to parsing tree 
    push @$tree, {type => 'text', tag_args => [$source]}
      if $source;
    
    return $tree;
}

sub _build_query {
    my ($self, $tree) = @_;
    
    # SQL
    my $sql = '';
    
    # All Columns
    my $all_columns = [];
    
    # Build SQL 
    foreach my $node (@$tree) {
        
        # Get type, tag name, and arguments
        my $type     = $node->{type};
        my $tag_name = $node->{tag_name};
        my $tag_args = $node->{tag_args};
        
        # Text
        if ($type eq 'text') {
            # Join text
            $sql .= $tag_args->[0];
        }
        
        # Tag
        elsif ($type eq 'tag') {
            
            # Get tag processor
            my $tag_processor = $self->tag_processors->{$tag_name};
            
            # Tag processor not sub reference
            croak qq{Tag processor "$tag_name" must be sub reference}
              unless ref $tag_processor eq 'CODE';
            
            # Execute tag processor
            my ($part, $columns) = @{$tag_processor->(@$tag_args)};
            
            # Check tag processor return value
            croak qq{Tag processor "$tag_name" must return [STRING, ARRAY_REFERENCE]}
              if !defined $part || ref $columns ne 'ARRAY';
            
            # Check placeholder count
            croak qq{Count of Placeholders must be same as count of columns in "$tag_name"}
              unless $self->_placeholder_count($part) eq @$columns;
            
            # Add columns
            push @$all_columns, @$columns;
            
            # Join part tag to SQL
            $sql .= $part;
        }
    }
    
    # Add semicolon
    $sql .= ';' unless $sql =~ /;$/;
    
    # Query
    my $query = DBIx::Custom::Query->new(sql => $sql, columns => $all_columns);
    
    return $query;
}

sub _placeholder_count {
    my ($self, $expand) = @_;
    
    # Count
    $expand ||= '';
    my $count = 0;
    my $pos   = -1;
    while (($pos = index($expand, '?', $pos + 1)) != -1) {
        $count++;
    }
    return $count;
}

1;

=head1 NAME

DBIx::Custom::QueryBuilder - Query builder

=head1 SYNOPSIS
    
    my $builder = DBIx::Custom::QueryBuilder->new;
    my $query = $builder->build_query(
        "select from table {= k1} && {<> k2} || {like k3}"
    );

=head1 ATTRIBUTES

=head2 C<tag_processors>

    my $tag_processors = $builder->tag_processors;
    $builder           = $builder->tag_processors(\%tag_processors);

Tag processors.

=head2 C<tag_start>
    
    my $tag_start = $builder->tag_start;
    $builder      = $builder->tag_start('{');

Tag start charactor.
Default to '{'.

=head2 C<tag_end>
    
    my $tag_end = $builder->tag_start;
    $builder    = $builder->tag_start('}');

Tag end charactor.
Default to '}'.
    
=head1 METHODS

L<DBIx::Custom::QueryBuilder> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<build_query>
    
    my $query = $builder->build_query($source);

Create a new L<DBIx::Custom::Query> object from SQL source.
SQL source contains tags, such as {= title}, {like author}.

B<Example:>

SQL source

      "select * from table where {= title} && {like author} || {<= price}"

Query

    {
        sql     => "select * from table where title = ? && author like ? price <= ?;"
        columns => ['title', 'author', 'price']
    }

=head2 C<register_tag_processor>

    $builder->register_tag_processor(\%tag_processors);
    $builder->register_tag_processor(%tag_processors);

Register tag processor.

B<Example:>

    $builder->register_tag_processor(
        '?' => sub {
            my $column = shift;
            
            return ['?', [$column]];
        }
    );

See also L<DBIx::Custom::QueryBuilder::TagProcessors> to know tag processor.

=head1 Tags

The following tags is available.
    
    [Tags]           [Replaced]
    {? NAME}    ->   ?
    {= NAME}    ->   NAME = ?
    {<> NAME}   ->   NAME <> ?
    
    {< NAME}    ->   NAME < ?
    {> NAME}    ->   NAME > ?
    {>= NAME}   ->   NAME >= ?
    {<= NAME}   ->   NAME <= ?
    
    {like NAME}       ->   NAME like ?
    {in NAME COUNT}   ->   NAME in [?, ?, ..]
    
    {insert NAME1 NAME2}   ->   (NAME1, NAME2) values (?, ?)
    {update NAME1 NAME2}   ->   set NAME1 = ?, NAME2 = ?
