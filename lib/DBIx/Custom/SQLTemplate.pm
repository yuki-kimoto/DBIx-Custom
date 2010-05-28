package DBIx::Custom::SQLTemplate;

use strict;
use warnings;

use base 'Object::Simple';

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::SQLTemplate::TagProcessor;

__PACKAGE__->attr('tag_processors' => sub { {} });

__PACKAGE__->attr(tag_start => '{');
__PACKAGE__->attr(tag_end   => '}');

__PACKAGE__->attr('tag_syntax' => <<'EOS');
[tag]                     [expand]
{? name}                  ?
{= name}                  name = ?
{<> name}                 name <> ?

{< name}                  name < ?
{> name}                  name > ?
{>= name}                 name >= ?
{<= name}                 name <= ?

{like name}               name like ?
{in name number}          name in [?, ?, ..]

{insert key1 key2} (key1, key2) values (?, ?)
{update key1 key2}    set key1 = ?, key2 = ?
EOS

sub new {
    my $self = shift->SUPER::new;
    
    $self->register_tag_processor(
        '?'      => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_placeholder_tag,
        '='      => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        '<>'     => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        '>'      => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        '<'      => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        '>='     => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        '<='     => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        'like'   => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_basic_tag,
        'in'     => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_in_tag,
        'insert' => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_insert_tag,
        'update' => \&DBIx::Custom::SQLTemplate::TagProcessors::expand_update_tag
    );
    
    return $self;
}

sub register_tag_processor {
    my $self = shift;
    my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->tag_processors({%{$self->tag_processors}, %{$tag_processors}});
    return $self;
}

sub create_query {
    my ($self, $template)  = @_;
    
    # Parse template
    my $tree = $self->_parse_template($template);
    
    # Build query
    my $query = $self->_build_query($tree);
    
    return $query;
}

sub _parse_template {
    my ($self, $template) = @_;
    
    if (ref $template eq 'ARRAY') {
        $template = $template->[1];
    }
    $template ||= '';
    
    my $tree = [];
    
    # Tags
    my $tag_start = quotemeta $self->tag_start;
    my $tag_end   = quotemeta $self->tag_end;
    
    # Tokenize
    my $state = 'text';
    
    # Save original template
    my $original_template = $template;
    
    # Parse template
    while ($template =~ s/([^$tag_start]*?)$tag_start([^$tag_end].*?)$tag_end//sm) {
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
                croak("Tag '{$tag}' in SQL template is not exist.\n\n" .
                      "<SQL template tag syntax>\n" .
                      "$tag_syntax\n" .
                      "<Your SQL template>\n" .
                      "$original_template\n\n");
            }
            
            # Check tag arguments
            foreach my $tag_arg (@tag_args) {
                # Cannot cantain placehosder '?'
                croak("Tag '{t }' arguments cannot contain '?'")
                  if $tag_arg =~ /\?/;
            }
            
            # Add tag to parsing tree
            push @$tree, {type => 'tag', tag_name => $tag_name, tag_args => [@tag_args]};
        }
    }
    
    # Add text to parsing tree 
    push @$tree, {type => 'text', tag_args => [$template]} if $template;
    
    return $tree;
}

sub _build_query {
    my ($self, $tree) = @_;
    
    # SQL
    my $sql = '';
    
    # All parameter key infomation
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
            croak("Tag processor '$tag_name' must be code reference")
              unless ref $tag_processor eq 'CODE';
            
            # Expand tag using tag processor
            my ($expand, $columns) = $tag_processor->($tag_name, $tag_args);
            
            # Check tag processor return value
            croak("Tag processor '$tag_name' must return (\$expand, \$columns)")
              if !defined $expand || ref $columns ne 'ARRAY';
            
            # Check placeholder count
            croak("Placeholder count in SQL created by tag processor '$tag_name' " .
                  "must be same as key informations count")
              unless $self->_placeholder_count($expand) eq @$columns;
            
            # Add key information
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

DBIx::Custom::SQLTemplate - DBIx::Custom SQL Template

=head1 SYNOPSIS
    
    my $sql_tmpl = DBIx::Custom::SQLTemplate->new;
    
    my $tmpl   = "select from table {= k1} && {<> k2} || {like k3}";
    my $param = {k1 => 1, k2 => 2, k3 => 3};
    
    my $query = $sql_template->create_query($tmpl);

=head1 ATTRIBUTES

=head2 tag_processors

    $sql_tmpl       = $sql_tmpl->tag_processors($name1 => $tag_processor1
                                                $name2 => $tag_processor2);
    $tag_processors = $sql_tmpl->tag_processors;

=head2 tag_start
    
    $sql_tmpl  = $sql_tmpl->tag_start('{');
    $tag_start = $sql_tmpl->tag_start;

Default is '{'

=head2 tag_end
    
    $sql_tmpl    = $sql_tmpl->tag_start('}');
    $tag_end = $sql_tmpl->tag_start;

Default is '}'
    
=head2 tag_syntax
    
    $sql_tmpl   = $sql_tmpl->tag_syntax($tag_syntax);
    $tag_syntax = $sql_tmpl->tag_syntax;

=head1 METHODS

This class is L<Object::Simple> subclass.
You can use all methods of L<Object::Simple>

=head2 new

=head2 create_query
    
Create L<DBIx::Custom::Query> object parsing SQL template

    $query = $sql_tmpl->create_query($tmpl);
    
    # Sample
    $query = $sql_tmpl->create_sql(
         "select * from table where {= title} && {like author} || {<= price}")
    
    # Expanded
    $qeury->sql : "select * from table where title = ? && author like ? price <= ?;"
    $query->key_infos : [['title'], ['author'], ['price']]
    
    # Sample with table name
    ($sql, @bind_values) = $sql_tmpl->create_sql(
            "select * from table where {= table.title} && {like table.author}",
            {table => {title => 'Perl', author => '%Taro%'}}
        )
    
    # Expanded
    $query->sql : "select * from table where table.title = ? && table.title like ?;"
    $query->key_infos :[ [['table.title'],['table', 'title']],
                         [['table.author'],['table', 'author']] ]

This method create query using by L<DBIx::Custom>.
query has two infomation

    1. sql       : SQL
    2. key_infos : Parameter access key information

=head2 register_tag_processor

Add tag processor
    
    $sql_tmpl = $sql_tmpl->register_tag_processor($tag_processor);

The following is register_tag_processor sample

    $sql_tmpl->register_tag_processor(
        '?' => sub {
            my ($tag_name, $tag_args) = @_;
            
            my $key1 = $tag_args->[0];
            my $key2 = $tag_args->[1];
            
            my $key_infos = [];
            
            # Expand tag and create key informations
            
            # Return expand tags and key informations
            return ($expand, $key_infos);
        }
    );

Tag processor recieve 2 argument

    1. Tag name            (?, =, <>, or etc)
    2. Tag arguments       (arg1 and arg2 in {tag_name arg1 arg2})

Tag processor return 2 value

    1. Expanded Tag (For exsample, '{= title}' is expanded to 'title = ?')
    2. Key infomations
    
You must be return expanded tag and key infomations.

Key information is a little complex. so I will explan this in future.

If you want to know more, Please see DBIx::Custom::SQLTemplate source code.

=head2 clone

Clone DBIx::Custom::SQLTemplate object

    $clone = $sql_tmpl->clone;
    
=head1 Available Tags
    
Available Tags

    [tag]            [expand]
    {? name}         ?
    {= name}         name = ?
    {<> name}        name <> ?
    
    {< name}         name < ?
    {> name}         name > ?
    {>= name}        name >= ?
    {<= name}        name <= ?
    
    {like name}      name like ?
    {in name}        name in [?, ?, ..]
    
    {insert}         (key1, key2, key3) values (?, ?, ?)
    {update}         set key1 = ?, key2 = ?, key3 = ?
    

The following is insert SQL sample

    $query = $sql_tmpl->create_sql(
        "insert into table {insert key1 key2}"
    );
    
    # Expanded
    $query->sql : "insert into table (key1, key2) values (?, ?)"

The following is update SQL sample
    
    $query = $sql_tmpl->create_sql(
        "update table {update key1 key2} where {= key3}"
    );
    
    # Expanded
    $query->sql : "update table set key1 = ?, key2 = ? where key3 = ?;"
    
=cut

