package DBIx::Custom::QueryBuilder;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder::TagProcessor;

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

__PACKAGE__->attr('tag_syntax' => <<'EOS');
[tag]                     [expand]
{? NAME}                  ?
{= NAME}                  NAME = ?
{<> NAME}                 NAME <> ?

{< NAME}                  NAME < ?
{> NAME}                  NAME > ?
{>= NAME}                 NAME >= ?
{<= NAME}                 NAME <= ?

{like NAME}               NAME like ?
{in NAME number}          NAME in [?, ?, ..]

{insert NAME1 NAME2}      (NAME1, NAME2) values (?, ?)
{update NAME1 NAME2}      set NAME1 = ?, NAME2 = ?
EOS

sub register_tag_processor {
    my $self = shift;
    
    # Merge
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
    
    $source ||= '';
    
    my $tree = [];
    
    # Tags
    my $tag_start = quotemeta $self->tag_start;
    my $tag_end   = quotemeta $self->tag_end;
    
    # Tokenize
    my $state = 'text';
    
    # Save original
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
            
            # Tag processor is exist?
            unless ($self->tag_processors->{$tag_name}) {
                my $tag_syntax = $self->tag_syntax;
                croak qq{Tag "{$tag}" is not registerd.\n\n} .
                      "<SQL builder syntax>\n" .
                      "$tag_syntax\n" .
                      "<Your source>\n" .
                      "$original\n\n";
            }
            
            # Check tag arguments
            foreach my $tag_arg (@tag_args) {
            
                # Cannot cantain placehosder '?'
                croak qq{Tag cannot contain "?"}
                  if $tag_arg =~ /\?/;
            }
            
            # Add tag to parsing tree
            push @$tree, {type => 'tag', tag_name => $tag_name, tag_args => [@tag_args]};
        }
    }
    
    # Add text to parsing tree 
    push @$tree, {type => 'text', tag_args => [$source]} if $source;
    
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
            
            # Tag processor is code ref?
            croak qq{Tag processor "$tag_name" must be code reference}
              unless ref $tag_processor eq 'CODE';
            
            # Expand tag using tag processor
            my ($expand, $columns) = @{$tag_processor->(@$tag_args)};
            
            # Check tag processor return value
            croak qq{Tag processor "$tag_name" must return [\$expand, \$columns]}
              if !defined $expand || ref $columns ne 'ARRAY';
            
            # Check placeholder count
            croak qq{Count of Placeholder must be same as count of columns in "$tag_name"}
              unless $self->_placeholder_count($expand) eq @$columns;
            
            # Add columns
            push @$all_columns, @$columns;
            
            # Join expand tag to SQL
            $sql .= $expand;
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
    
    my $source = "select from table {= k1} && {<> k2} || {like k3}";
    my $param = {k1 => 1, k2 => 2, k3 => 3};
    
    my $query = $sql_builder->build_query($source);

=head1 ATTRIBUTES

=head2 C<tag_processors>

    my $tag_processors = $builder->tag_processors;
    $builder           = $builder->tag_processors(\%tag_processors);

Tag processors.

=head2 C<tag_start>
    
    my $tag_start = $builder->tag_start;
    $builder      = $builder->tag_start('{');

String of tag start.
Default to '{'

=head2 C<tag_end>
    
    my $tag_end = $builder->tag_start;
    $builder    = $builder->tag_start('}');

String of tag end.
Default to '}'
    
=head2 C<tag_syntax>
    
    my $tag_syntax = $builder->tag_syntax;
    $builder       = $builder->tag_syntax($tag_syntax);

Tag syntax.

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 C<new>

    my $builder = DBIx::Custom::SQLBuilder->new;
    my $builder = DBIx::Custom::SQLBuilder->new(%attrs);
    my $builder = DBIx::Custom::SQLBuilder->new(\%attrs);

Create a instance.

=head2 C<build_query>
    
    my $query = $builder->build_query($source);

Build L<DBIx::Custom::Query> object.

B<Example:>

Source:

    my $query = $builder->build_query(
      "select * from table where {= title} && {like author} || {<= price}")

Query:

    $qeury->sql : "select * from table where title = ? && author like ? price <= ?;"
    $query->columns : ['title', 'author', 'price']

=head2 C<register_tag_processor>

    $builder = $builder->register_tag_processor($tag_processor);

Register tag processor.

    $builder->register_tag_processor(
        '?' => sub {
            my $args = shift;
            
            # Do something
            
            # Expanded tag and column names
            return ($expand, $columns);
        }
    );

Tag processor receive arguments in tags
and must return expanded tag and column names.

=head1 Tags

    {? NAME}    ->   ?
    {= NAME}    ->   NAME = ?
    {<> NAME}   ->   NAME <> ?
    
    {< NAME}    ->   NAME < ?
    {> NAME}    ->   NAME > ?
    {>= NAME}   ->   NAME >= ?
    {<= NAME}   ->   NAME <= ?
    
    {like NAME}       ->   NAME like ?
    {in NAME COUNT}   ->   NAME in [?, ?, ..]
    
    {insert NAME1 NAME2 NAME3}   ->   (NAME1, NAME2, NAME3) values (?, ?, ?)
    {update NAME1 NAME2 NAME3}   ->   set NAME1 = ?, NAME2 = ?, NAME3 = ?
