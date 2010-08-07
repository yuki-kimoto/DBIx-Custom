package DBIx::Custom::QueryBuilder;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder::TagProcessors;

__PACKAGE__->dual_attr('tag_processors', default => sub { {} }, inherit => 'hash_copy');
__PACKAGE__->register_tag_processor(
    '?'      => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_placeholder_tag,
    '='      => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_equal_tag,
    '<>'     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_not_equal_tag,
    '>'      => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_greater_than_tag,
    '<'      => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_lower_than_tag,
    '>='     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_greater_than_equal_tag,
    '<='     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_lower_than_equal_tag,
    'like'   => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_like_tag,
    'in'     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_in_tag,
    'insert' => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_insert_tag,
    'update' => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_update_tag
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
            
            # Tag processor not registered
            croak qq{Tag "$tag_name" in "$original" is not registered}
               unless $self->tag_processors->{$tag_name};
            
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
            my $r = $tag_processor->(@$tag_args);
            
            # Check tag processor return value
            croak qq{Tag processor "$tag_name" must return [STRING, ARRAY_REFERENCE]}
              unless ref $r eq 'ARRAY' && defined $r->[0] && ref $r->[1] eq 'ARRAY';
            
            # Part of SQL statement and colum names
            my ($part, $columns) = @$r;
            
            # Add columns
            push @$all_columns, @$columns;
            
            # Join part tag to SQL
            $sql .= $part;
        }
    }

    # Check placeholder count
    my $placeholder_count = $self->_placeholder_count($sql);
    my $column_count      = @$all_columns;
    croak qq{Placeholder count in "$sql" must be same as column count $column_count}
      unless $placeholder_count eq @$all_columns;
    
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

=head2 C<?>

Placeholder tag.

    {? NAME}    ->   ?

=head2 C<=>

Equal tag.

    {= NAME}    ->   NAME = ?

=head2 C<E<lt>E<gt>>

Not equal tag.

    {<> NAME}   ->   NAME <> ?

=head2 C<E<lt>>

Lower than tag

    {< NAME}    ->   NAME < ?

=head2 C<E<gt>>

Greater than tag

    {> NAME}    ->   NAME > ?

=head2 C<E<gt>=>

Greater than or equal tag

    {>= NAME}   ->   NAME >= ?

=head2 C<E<lt>=>

Lower than or equal tag

    {<= NAME}   ->   NAME <= ?

=head2 C<like>

Like tag

    {like NAME}   ->   NAME like ?

=head2 C<in>

In tag.

    {in NAME COUNT}   ->   NAME in [?, ?, ..]

=head2 C<insert>

Insert tag.

    {insert NAME1 NAME2}   ->   (NAME1, NAME2) values (?, ?)

=head2 C<update>

Updata tag.

    {update NAME1 NAME2}   ->   set NAME1 = ?, NAME2 = ?
