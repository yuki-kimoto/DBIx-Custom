package DBIx::Custom::QueryBuilder;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::QueryBuilder::TagProcessors;

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

# Attributes
__PACKAGE__->attr('tag_processors' => sub {
    {
        '?'     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_placeholder_tag,
        '='     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_equal_tag,
        '<>'    => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_not_equal_tag,
        '>'     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_greater_than_tag,
        '<'     => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_lower_than_tag,
        '>='    => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_greater_than_equal_tag,
        '<='    => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_lower_than_equal_tag,
        'like'  => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_like_tag,
        'in'    => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_in_tag,
        'insert_param' => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_insert_param_tag,
        'update_param' => \&DBIx::Custom::QueryBuilder::TagProcessors::expand_update_param_tag
    }
});

sub build_query {
    my ($self, $source)  = @_;
    
    # Parse
    my $tree = $self->_parse($source);
    
    # Build query
    my $query = $self->_build_query($tree);
    
    return $query;
}

sub register_tag_processor {
    my $self = shift;
    
    # Merge tag processor
    my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->tag_processors({%{$self->tag_processors}, %{$tag_processors}});
    
    return $self;
}

sub _build_query {
    my ($self, $tree) = @_;
    
    # SQL
    my $sql = '';
    
    # All Columns
    my $all_columns = [];
    
    # Build SQL 
    foreach my $node (@$tree) {
        
        # Text
        if ($node->{type} eq 'text') { $sql .= $node->{value} }
        
        # Tag
        else {
            
            # Tag name
            my $tag_name = $node->{tag_name};
            
            # Tag arguments
            my $tag_args = $node->{tag_args};
            
            # Get tag processor
            my $tag_processor = $self->tag_processors->{$tag_name};
            
            # Tag processor is not registered
            croak qq{Tag "$tag_name" in "{a }" is not registered}
              unless $tag_processor;
            
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

sub _parse {
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
    while (defined(my $c = substr($source, $pos, 1))) {
        
        # Last
        last unless length $c;
        
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
                    croak qq/Parsing error. unexpected "}". / .
                          qq/pos $pos of "$original"/;
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
                    croak qq/Parsing error. unexpected "{". / .
                          qq/pos $pos of "$original"/;
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
    croak qq{Tag not finished. "$original"}
      if $state eq 'tag';
    
    # Add rest text
    push @tree, {type => 'text', value => $value}
      if $value;
    
    return \@tree;
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

=head1 METHODS

L<DBIx::Custom::QueryBuilder> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<build_query>
    
    my $query = $builder->build_query($source);

Create a new L<DBIx::Custom::Query> object from SQL source.
SQL source contains tags, such as {= title}, {like author}.

C<{> and C<}> is reserved. If you use these charactors,
you must escape them using '\'. Note that '\' is
already perl escaped charactor, so you must write '\\'. 

    'select * from books \\{ something statement \\}'

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

=head2 C<insert_param>

Insert parameter tag.

    {insert_param NAME1 NAME2}   ->   (NAME1, NAME2) values (?, ?)

=head2 C<update_param>

Updata parameter tag.

    {update_param NAME1 NAME2}   ->   set NAME1 = ?, NAME2 = ?
