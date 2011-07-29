package DBIx::Custom::QueryBuilder;

use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::Util '_subname';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;
push @DBIx::Custom::Where::CARP_NOT, __PACKAGE__;

has 'safety_character';

sub build_query {
    my ($self, $source) = @_;
    
    my $query;
    
    # Parse tag. tag is DEPRECATED!
    $self->{_tag_parse} = 1 unless defined $self->{_tag_parse};
    if ($self->{_tag_parse} && $source =~ /\{/ && $source =~ /\}/) {
        $query = $self->_parse_tag($source);
        my $tag_count = delete $query->{tag_count};
        warn qq/Tag system such as {? name} is DEPRECATED! / .
             qq/use parameter system such as :name instead/
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
    }
    
    # Parse parameter
    else { $query = $self->_parse_parameter($source) }
    
    my $sql = $query->sql;
    $sql .= ';' unless $source =~ /;$/;
    $query->sql($sql);

    # Check placeholder count
    croak qq{Placeholder count in "$sql" must be same as column count}
        . _subname
      unless $self->_placeholder_count($sql) eq @{$query->columns};
        
    return $query;
}

sub _placeholder_count {
    my ($self, $sql) = @_;
    
    # Count
    $sql ||= '';
    my $count = 0;
    my $pos   = -1;
    while (($pos = index($sql, '?', $pos + 1)) != -1) {
        $count++;
    }
    return $count;
}

sub _parse_parameter {
    my ($self, $source) = @_;

    # Get and replace parameters
    my $sql = $source || '';
    my $columns = [];
    my $c = $self->safety_character;
    # Parameter regex
    my $re = qr/^(.*?):([$c\.]+)(?:\{(.*?)\})?(.*)/s;
    while ($sql =~ /$re/g) {
        push @$columns, $2;
        $sql = defined $3 ? "$1$2 $3 ?$4" : "$1?$4";
    }

    # Create query
    my $query = DBIx::Custom::Query->new(
        sql => $sql,
        columns => $columns
    );
    
    return $query;
}

# DEPRECATED!
has tags => sub { {} };

# DEPRECATED!
sub register_tag {
    my $self = shift;
    
    warn "register_tag is DEPRECATED!";
    
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
    foreach my $node (@tree) {
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
            my $tag = $self->tag_processors->{$tag_name}
                             || $self->tags->{$tag_name};
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
    warn "register_tag_processor is DEPRECATED!";
    # Merge tag
    my $tag_processors = ref $_[0] eq 'HASH' ? $_[0] : {@_};
    $self->tag_processors({%{$self->tag_processors}, %{$tag_processors}});
    return $self;
}

1;

=head1 NAME

DBIx::Custom::QueryBuilder - Query builder

=head1 SYNOPSIS
    
    my $builder = DBIx::Custom::QueryBuilder->new;
    my $query = $builder->build_query(
        "select from table title = :title and author = :author"
    );

=head1 METHODS

L<DBIx::Custom::QueryBuilder> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<build_query>
    
    my $query = $builder->build_query($source);

Create a new L<DBIx::Custom::Query> object from SQL source.

=cut
