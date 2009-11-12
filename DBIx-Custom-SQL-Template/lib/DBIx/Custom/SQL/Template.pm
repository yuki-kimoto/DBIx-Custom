package DBIx::Custom::SQL::Template;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';

# Accessor is created by Object::Simple. Please read Object::Simple document

### Class-Object accessors

# Tag start
sub tag_start   : ClassObjectAttr {
    initialize => {default => '{', clone => 'scalar'}
}

# Tag end
sub tag_end     : ClassObjectAttr {
    initialize => {default => '}', clone => 'scalar'}
}

# Tag syntax
sub tag_syntax  : ClassObjectAttr {
    initialize => {default => <<'EOS', clone => 'scalar'}}
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

# Tag processors
sub tag_processors : ClassObjectAttr {
    type => 'hash',
    deref => 1,
    initialize => {
        clone => 'hash', 
        default => sub {{
            '?'             => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            '='             => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            '<>'            => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            '>'             => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            '<'             => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            '>='            => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            '<='            => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            'like'          => \&DBIx::Custom::SQL::Template::TagProcessor::expand_basic_tag,
            'in'            => \&DBIx::Custom::SQL::Template::TagProcessor::expand_in_tag,
            'insert'        => \&DBIx::Custom::SQL::Template::TagProcessor::expand_insert_tag,
            'update'    => \&DBIx::Custom::SQL::Template::TagProcessor::expand_update_tag
        }}
    }
}

# Add Tag processor
sub add_tag_processor {
    my $invocant = shift;
    my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $invocant->tag_processors(%{$invocant->tag_processors}, %{$tag_processors});
    return $invocant;
}

# Clone
sub clone {
    my $self = shift;
    my $new = $self->new;
    
    $new->tag_start($self->tag_start);
    $new->tag_end($self->tag_end);
    $new->tag_syntax($self->tag_syntax);
    $new->tag_processors({%{$self->tag_processors || {}}});
    
    return $new;
}


### Object Methods

# Create Query
sub create_query {
    my ($self, $template)  = @_;
    
    # Parse template
    my $tree = $self->_parse_template($template);
    
    # Build query
    my $query = $self->_build_query($tree);
    
    return $query;
}

# Parse template
sub _parse_template {
    my ($self, $template) = @_;
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

# Build SQL from parsing tree
sub _build_query {
    my ($self, $tree) = @_;
    
    # SQL
    my $sql = '';
    
    # All parameter key infomation
    my $all_key_infos = [];
    
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
            my ($expand, $key_infos)
              = $tag_processor->($tag_name, $tag_args);
            
            # Check tag processor return value
            croak("Tag processor '$tag_name' must return (\$expand, \$key_infos)")
              if !defined $expand || ref $key_infos ne 'ARRAY';
            
            # Check placeholder count
            croak("Placeholder count in SQL created by tag processor '$tag_name' " .
                  "must be same as key informations count")
              unless $self->_placeholder_count($expand) eq @$key_infos;
            
            # Add key information
            push @$all_key_infos, @$key_infos;
            
            # Join expand tag to SQL
            $sql .= $expand;
        }
    }
    
    # Add semicolon
    $sql .= ';' unless $sql =~ /;$/;
    
    # Query
    my $query = {sql => $sql, key_infos => $all_key_infos};
    
    return $query;
}

# Get placeholder count
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

Object::Simple->build_class;


package DBIx::Custom::SQL::Template::TagProcessor;
use strict;
use warnings;
use Carp 'croak';

# Expand tag '?', '=', '<>', '>', '<', '>=', '<=', 'like'
sub expand_basic_tag {
    my ($tag_name, $tag_args) = @_;
    my $original_key = $tag_args->[0];
    
    # Key is not exist
    croak("You must be pass key as argument to tag '{$tag_name }'")
      if !$original_key;
    
    # Expanded tag
    my $expand = $tag_name eq '?'
               ? '?'
               : "$original_key $tag_name ?";
    
    # Get table and clumn name
    my ($table, $column) = get_table_and_column($original_key);
    
    # Parameter key infomation
    my $key_info = {};
    
    # Original key
    $key_info->{original_key} = $original_key;
    
    # Table
    $key_info->{table}  = $table;
    
    # Column name
    $key_info->{column} = $column;
    
    # Access keys
    my $access_keys = [];
    push @$access_keys, [$original_key];
    push @$access_keys, [$table, $column] if $table && $column;
    $key_info->{access_keys} = $access_keys;
    
    # Add parameter key information
    my $key_infos = [];
    push @$key_infos, $key_info;
    
    return ($expand, $key_infos);
}

# Expand tag 'in'
sub expand_in_tag {
    my ($tag_name, $tag_args) = @_;
    my ($original_key, $placeholder_count) = @$tag_args;
    
    # Key must be specified
    croak("You must be pass key as first argument of tag '{$tag_name }'\n" . 
          "Usage: {$tag_name \$key \$placeholder_count}")
      unless $original_key;
      
    
    # Place holder count must be specified
    croak("You must be pass placeholder count as second argument of tag '{$tag_name }'\n" . 
          "Usage: {$tag_name \$key \$placeholder_count}")
      if !$placeholder_count || $placeholder_count =~ /\D/;

    # Expand tag
    my $expand = "$original_key $tag_name (";
    for (my $i = 0; $i < $placeholder_count; $i++) {
        $expand .= '?, ';
    }
    
    $expand =~ s/, $//;
    $expand .= ')';
    
    # Get table and clumn name
    my ($table, $column) = get_table_and_column($original_key);
    
    # Create parameter key infomations
    my $key_infos = [];
    for (my $i = 0; $i < $placeholder_count; $i++) {
        # Parameter key infomation
        my $key_info = {};
        
        # Original key
        $key_info->{original_key} = $original_key;
        
        # Table
        $key_info->{table}   = $table;
        
        # Column name
        $key_info->{column}  = $column;
        
        # Access keys
        my $access_keys = [];
        push @$access_keys, [$original_key, [$i]];
        push @$access_keys, [$table, $column, [$i]] if $table && $column;
        $key_info->{access_keys} = $access_keys;
        
        # Add parameter key infos
        push @$key_infos, $key_info;
    }
    
    return ($expand, $key_infos);
}

# Get table and column
sub get_table_and_column {
    my $key = shift;
    $key ||= '';
    
    return ('', $key) unless $key =~ /\./;
    
    my ($table, $column) = split /\./, $key;
    
    return ($table, $column);
}

# Expand tag 'insert'
sub expand_insert_tag {
    my ($tag_name, $tag_args) = @_;
    my $original_keys = $tag_args;
    
    # Insert key (k1, k2, k3, ..)
    my $insert_keys = '(';
    
    # placeholder (?, ?, ?, ..)
    my $place_holders = '(';
    
    foreach my $original_key (@$original_keys) {
        # Get table and column
        my ($table, $column) = get_table_and_column($original_key);
        
        # Join insert column
        $insert_keys   .= "$column, ";
        
        # Join place holder
        $place_holders .= "?, ";
    }
    
    # Delete last ', '
    $insert_keys =~ s/, $//;
    
    # Close 
    $insert_keys .= ')';
    $place_holders =~ s/, $//;
    $place_holders .= ')';
    
    # Expand tag
    my $expand = "$insert_keys values $place_holders";
    
    # Create parameter key infomations
    my $key_infos = [];
    foreach my $original_key (@$original_keys) {
        # Get table and clumn name
        my ($table, $column) = get_table_and_column($original_key);
        
        # Parameter key infomation
        my $key_info = {};
        
        # Original key
        $key_info->{original_key} = $original_key;
        
        # Table
        $key_info->{table}   = $table;
        
        # Column name
        $key_info->{column}  = $column;
        
        # Access keys
        my $access_keys = [];
        push @$access_keys, ['#insert', $original_key];
        push @$access_keys, ['#insert', $table, $column] if $table && $column;
        push @$access_keys, [$original_key];
        push @$access_keys, [$table, $column] if $table && $column;
        $key_info->{access_keys} = $access_keys;
        
        # Add parameter key infos
        push @$key_infos, $key_info;
    }
    
    return ($expand, $key_infos);
}

# Expand tag 'update'
sub expand_update_tag {
    my ($tag_name, $tag_args) = @_;
    my $original_keys = $tag_args;
    
    # Expanded tag
    my $expand = 'set ';
    
    # 
    foreach my $original_key (@$original_keys) {
        # Get table and clumn name
        my ($table, $column) = get_table_and_column($original_key);

        # Join key and placeholder
        $expand .= "$column = ?, ";
    }
    
    # Delete last ', '
    $expand =~ s/, $//;
    
    # Create parameter key infomations
    my $key_infos = [];
    foreach my $original_key (@$original_keys) {
        # Get table and clumn name
        my ($table, $column) = get_table_and_column($original_key);
        
        # Parameter key infomation
        my $key_info = {};
        
        # Original key
        $key_info->{original_key} = $original_key;
        
        # Table
        $key_info->{table}  = $table;
        
        # Column name
        $key_info->{column} = $column;
        
        # Access keys
        my $access_keys = [];
        push @$access_keys, ['#update', $original_key];
        push @$access_keys, ['#update', $table, $column] if $table && $column;
        push @$access_keys, [$original_key];
        push @$access_keys, [$table, $column] if $table && $column;
        $key_info->{access_keys} = $access_keys;
        
        # Add parameter key infos
        push @$key_infos, $key_info;
    }
    
    return ($expand, $key_infos);
}

1;

=head1 NAME

DBIx::Custom::SQL::Template - Custamizable SQL Template for DBIx::Custom

=head1 VERSION

Version 0.0101

=cut

=head1 SYNOPSIS
    
    my $sql_tmpl = DBIx::Custom::SQL::Template->new;
    
    my $tmpl   = "select from table {= k1} && {<> k2} || {like k3}";
    my $param = {k1 => 1, k2 => 2, k3 => 3};
    
    my $query = $sql_template->create_query($tmpl);
    
    
    # Using query from DBIx::Custom
    use DBIx::Custom;
    my $dbi = DBI->new(
       data_source => $data_source,
       user        => $user,
       password    => $password, 
       dbi_options => {PrintError => 0, RaiseError => 1}
    );
    
    $query = $dbi->create_query($tmpl); # This is SQL::Template create_query
    $dbi->query($query, $param);

=head1 CLASS-OBJECT ACCESSORS

Class-Object accessor is used from both object and class

    $class->$accessor # call from class
    $self->$accessor  # call form object

=head2 tag_processors

    # Set and get
    $self           = $sql_tmpl->tag_processors($tag_processors);
    $tag_processors = $sql_tmpl->tag_processors;
    
    # Sample
    $sql_tmpl->tag_processors(
        '?' => \&expand_question,
        '=' => \&expand_equel
    );

You can use add_tag_processor to add tag processor

=head2 tag_start

    # Set and get
    $self      = $sql_tmpl->tag_start($tag_start);
    $tag_start = $sql_tmpl->tag_start;
    
    # Sample
    $sql_tmpl->tag_start('{');

Default is '{'

=head2 tag_end

    # Set and get
    $self    = $sql_tmpl->tag_start($tag_end);
    $tag_end = $sql_tmpl->tag_start;
    
    # Sample
    $sql_tmpl->tag_start('}');

Default is '}'
    
=head2 tag_syntax
    
    # Set and get
    $self       = $sql_tmpl->tag_syntax($tag_syntax);
    $tag_syntax = $sql_tmpl->tag_syntax;
    
    # Sample
    $sql_tmpl->tag_syntax(
        "[Tag]            [Expand]\n" .
        "{? name}         ?\n" .
        "{= name}         name = ?\n" .
        "{<> name}        name <> ?\n"
    );

=head1 METHODS

=head2 create_query
    
    # Create SQL form SQL template
    $query = $sql_tmpl->create_query($tmpl);
    
    # Sample
    $query = $sql_tmpl->create_sql(
         "select * from table where {= title} && {like author} || {<= price}")
    
    # Result
    $qeury->{sql} : "select * from table where title = ? && author like ? price <= ?;"
    $query->{key_infos} : [['title'], ['author'], ['price']]
    
    # Sample2 (with table name)
    ($sql, @bind_values) = $sql_tmpl->create_sql(
            "select * from table where {= table.title} && {like table.author}",
            {table => {title => 'Perl', author => '%Taro%'}}
        )
    
    # Result2
    $query->{sql} : "select * from table where table.title = ? && table.title like ?;"
    $query->{key_infos} :[ [['table.title'],['table', 'title']],
                           [['table.author'],['table', 'author']] ]

This method create query using by DBIx::Custom.
query is two infomation

    1.sql       : SQL
    2.key_infos : Parameter access key information

=head2 add_tag_processor

Add tag processor
  
    # Add
    $self = $sql_tmpl->add_tag_processor($tag_processor);
    
    # Sample
    $sql_tmpl->add_tag_processor(
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

If you want to know more, Please see DBIx::Custom::SQL::Template source code.

=head2 clone

    # Clone DBIx::Custom::SQL::Template object
    $clone = $self->clone;
    
=head1 Available Tags
    
    # Available Tags
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
    
    {insert}  (key1, key2, key3) values (?, ?, ?)
    {update}     set key1 = ?, key2 = ?, key3 = ?
    
    # Sample1
    $query = $sql_tmpl->create_sql(
        "insert into table {insert key1 key2}"
    );
    # Result1
    $sql : "insert into table (key1, key2) values (?, ?)"
    
    
    # Sample2
    $query = $sql_tmpl->create_sql(
        "update table {update key1 key2} where {= key3}"
    );
    
    # Result2
    $query->{sql} : "update table set key1 = ?, key2 = ? where key3 = ?;"
    
=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github 
L<http://github.com/yuki-kimoto>
L<http://github.com/yuki-kimoto/DBIx-Custom-SQL-Template>

Please let know me bag if you find
Please request me if you want to do something

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of DBIx::Custom::SQL::Template
