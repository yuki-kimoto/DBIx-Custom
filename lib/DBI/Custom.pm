package DBI::Custom;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';
use DBI;

# Model
sub prototype : ClassAttr { auto_build => \&_inherit_prototype }

# Inherit super class prototype
sub _inherit_prototype {
    my $class = shift;
    my $super = do {
        no strict 'refs';
        ${"${class}::ISA"}[0];
    };
    my $prototype = eval{$super->can('prototype')}
                         ? $super->prototype->clone
                         : $class->Object::Simple::new;
    
    $class->prototype($prototype);
}

# New
sub new {
    my $self = shift->Object::Simple::new(@_);
    my $class = ref $self;
    return bless {%{$class->prototype->clone}, %{$self}}, $class;
}

# Initialize class
sub initialize_class {
    my ($class, $callback) = @_;
    
    # Callback to initialize prototype
    $callback->($class->prototype);
}

# Clone
sub clone {
    my $self = shift;
    my $new = $self->Object::Simple::new;
    $new->connect_info(%{$self->connect_info || {}});
    $new->filters(%{$self->filters || {}});
    $new->bind_filter($self->bind_filter);
    $new->fetch_filter($self->fetch_filter);
    $new->result_class($self->result_class);
}

# Attribute
sub connect_info       : Attr { type => 'hash',  auto_build => sub { shift->connect_info({}) } }

sub bind_filter  : Attr {}
sub fetch_filter : Attr {}

sub filters : Attr { type => 'hash', deref => 1, auto_build => sub { shift->filters({}) } }
sub add_filter { shift->filters(@_) }

sub result_class : Attr { auto_build => sub { shift->result_class('DBI::Custom::Result') }}
sub dbh          : Attr {}
sub sql_template : Attr { auto_build => sub { shift->sql_template(DBI::Custom::SQL::Template->new) } }

# Auto commit
sub auto_commit {
    my $self = shift;
    
    croak("Cannot change AutoCommit becouse of not connected")
        unless $self->dbh;
    
    if (@_) {
        $self->dbh->{AutoCommit} = $_[0];
        return $self;
    }
    return $self->dbh->{AutoCommit};
}


our %VALID_CONNECT_INFO = map {$_ => 1} qw/data_source user password options/;

# Connect
sub connect {
    my $self = shift;
    my $connect_info = $self->connect_info;
    
    foreach my $key (keys %{$self->connect_info}) {
        croak("connect_info '$key' is wrong name")
          unless $VALID_CONNECT_INFO{$key};
    }
    
    my $dbh = DBI->connect(
        $connect_info->{data_source},
        $connect_info->{user},
        $connect_info->{password},
        {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            %{$connect_info->{options} || {} }
        }
    );
    
    $self->dbh($dbh);
    return $self;
}

sub DESTROY {
    my $self = shift;
    $self->disconnect if $self->connected;
}

# Is connected?
sub connected {
    my $self = shift;
    return exists $self->{dbh} && eval {$self->{dbh}->can('prepare')};
}

# Disconnect
sub disconnect {
    my $self = shift;
    if ($self->connected) {
        $self->dbh->disconnect;
        delete $self->{dbh};
    }
}

# Reconnect
sub reconnect {
    my $self = shift;
    $self->disconnect if $self->connected;
    $self->connect;
}

# Commit
sub commit {
    my $self = shift;
    croak("Connection is not established") unless $self->connected;
    return $self->dbh->commit;
}

# Rollback
sub rollback {
    my $self = shift;
    croak("Connection is not established") unless $self->connected;
    return $self->dbh->rollback;
}

sub dbh_option {
    my $self = shift;
    croak("Not connected") unless $self->connected;
    my $dbh = $self->dbh;
    if (@_ > 1) {
        $dbh->{$_[0]} = $_[1];
        return $self;
    }
    return $dbh->{$_[0]}
}

# Create SQL from SQL template
sub create_sql {
    my $self = shift;
    
    my ($sql, @bind) = $self->sql_template->create_sql(@_);
    
    return ($sql, @bind);
}

# Prepare and execute SQL
sub query {
    my ($self, $template, $values, $filter)  = @_;
    
    my $sth_options;
    
    # Rearrange when argumets is hash referecne 
    if (ref $template eq 'HASH') {
        my $args = $template;
        ($template, $values, $filter, $sth_options)
          = @{$args}{qw/template values filter sth_options/};
    }
    
    $filter ||= $self->bind_filter;
    
    my ($sql, @bind) = $self->create_sql($template, $values, $filter);
    
    $self->connect unless $self->connected;
    
    my $sth = $self->dbh->prepare($sql);
    
    if ($sth_options) {
        foreach my $key (keys %$sth_options) {
            $sth->{$key} = $sth_options->{$key};
        }
    }
    
    # Execute
    my $ret_val = $sth->execute(@bind);
    
    # Return resultset if select statement is executed
    if ($sth->{NUM_OF_FIELDS}) {
        my $result_class = $self->result_class;
        my $result = $result_class->new({
            sth => $sth,
            fetch_filter => $self->fetch_filter
        });
        return $result;
    }
    return $ret_val;
}

# Prepare and execute raw SQL
sub query_raw_sql {
    my ($self, $sql, @bind_values) = @_;
    
    # Connect
    $self->connect unless $self->connected;
    
    # Add semicolon if not exist;
    $sql .= ';' unless $sql =~ /;$/;
    
    # Prepare
    my $sth = $self->dbh->prepare($sql);
    
    # Execute
    $sth->execute(@bind_values);
    
    return $sth;
}

Object::Simple->build_class;

package DBI::Custom::Result;
use Object::Simple;

# Attributes
sub sth          : Attr {}
sub fetch_filter : Attr {}


# Fetch (array)
sub fetch {
    my ($self, $type) = @_;
    my $sth = $self->sth;
    my $fetch_filter = $self->fetch_filter;
    
    # Fetch
    my $row = $sth->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;
    
    # Filter
    if ($fetch_filter) {
        my $keys  = $sth->{NAME_lc};
        my $types = $sth->{TYPE};
        for (my $i = 0; $i < @$keys; $i++) {
            $row->[$i]= $fetch_filter->($keys->[$i], $row->[$i], $types->[$i],
                                        $sth, $i);
        }
    }
    return wantarray ? @$row : $row;
}

# Fetch (hash)
sub fetch_hash {
    my $self = shift;
    my $sth = $self->sth;
    my $fetch_filter = $self->fetch_filter;
    
    # Fetch
    my $row = $sth->fetchrow_arrayref;
    
    # Cannot fetch
    return unless $row;
    
    # Keys
    my $keys  = $sth->{NAME_lc};
    
    # Filter
    my $row_hash = {};
    if ($fetch_filter) {
        my $types = $sth->{TYPE};
        for (my $i = 0; $i < @$keys; $i++) {
            $row_hash->{$keys->[$i]} = $fetch_filter->($keys->[$i], $row->[$i],
                                                       $types->[$i], $sth, $i);
        }
    }
    
    # No filter
    else {
        for (my $i = 0; $i < @$keys; $i++) {
            $row_hash->{$keys->[$i]} = $row->[$i];
        }
    }
    return wantarray ? %$row_hash : $row_hash;
}

# Fetch all (array)
sub fetch_all {
    my $self = shift;
    
    my $rows = [];
    while(my @row = $self->fetch) {
        push @$rows, [@row];
    }
    return wantarray ? @$rows : $rows;
}

# Fetch all (hash)
sub fetch_all_hash {
    my $self = shift;
    
    my $rows = [];
    while(my %row = $self->fetch_hash) {
        push @$rows, {%row};
    }
    return wantarray ? @$rows : $rows;
}

# Finish
sub finish { shift->sth->finish }

# Error
sub error { 
    my $self = shift;
    my $sth  = $self->sth;
    wantarray ? ($sth->errstr, $sth->err, $sth->state) : $sth->errstr;
}

Object::Simple->build_class;


package DBI::Custom::SQL::Template;
use Object::Simple;
use Carp 'croak';

### Attributes;
sub tag_start   : Attr { default => '{' }
sub tag_end     : Attr { default => '}' }
sub template    : Attr {};
sub tree        : Attr { auto_build => sub { shift->tree([]) } }
sub bind_filter : Attr {}
sub values      : Attr {}
sub upper_case  : Attr {default => 0}

sub create_sql {
    my ($self, $template, $values, $filter)  = @_;
    
    $filter ||= $self->bind_filter;
    
    $self->parse($template);
    
    my ($sql, @bind) = $self->build_sql({bind_filter => $filter, values => $values});
    
    return ($sql, @bind);
}

our $TAG_SYNTAX = <<'EOS';
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

{insert_values}  (key1, key2, key3) values (?, ?, ?)
{update_values}  set key1 = ?, key2 = ?, key3 = ?
EOS

our %VALID_TAG_NAMES = map {$_ => 1} qw/= <> < > >= <= like in insert_values update_set/;
sub parse {
    my ($self, $template) = @_;
    $self->template($template);
    
    # Clean start;
    delete $self->{tree};
    
    # Tags
    my $tag_start = quotemeta $self->tag_start;
    my $tag_end   = quotemeta $self->tag_end;
    
    # Tokenize
    my $state = 'text';
    
    # Save original template
    my $original_template = $template;
    
    # Text
    while ($template =~ s/([^$tag_start]*?)$tag_start([^$tag_end].*?)$tag_end//sm) {
        my $text = $1;
        my $tag  = $2;
        
        push @{$self->tree}, {type => 'text', args => [$text]} if $text;
        
        if ($tag) {
            
            my ($tag_name, @args) = split /\s+/, $tag;
            
            $tag ||= '';
            croak("Tag '$tag' in SQL template is not exist.\n\n" .
                  "SQL template tag syntax\n$TAG_SYNTAX\n\n" .
                  "Your SQL template is \n$original_template\n\n")
              unless $VALID_TAG_NAMES{$tag_name};
            
            push @{$self->tree}, {type => 'tag', tag_name => $tag_name, args => [@args]};
        }
    }
    
    push @{$self->tree}, {type => 'text', args => [$template]} if $template;
}

our %EXPAND_PLACE_HOLDER = map {$_ => 1} qw/= <> < > >= <= like/;
sub build_sql {
    my ($self, $args) = @_;
    
    my $tree        = $args->{tree} || $self->tree;
    my $bind_filter = $args->{bind_filter} || $self->bind_filter;
    my $values      = exists $args->{values} ? $args->{values} : $self->values;
    
    my @bind_values;
    my $sql = '';
    foreach my $node (@$tree) {
        my $type     = $node->{type};
        my $tag_name = $node->{tag_name};
        my $args     = $node->{args};
        
        if ($type eq 'text') {
            # Join text
            $sql .= $args->[0];
        }
        elsif ($type eq 'tag') {
            if ($EXPAND_PLACE_HOLDER{$tag_name}) {
                my $key = $args->[0];
                
                # Filter Value
                if ($bind_filter) {
                    push @bind_values, scalar $bind_filter->($key, $values->{$key});
                }
                else {
                    push @bind_values, $values->{$key};
                }
                $tag_name = uc $tag_name if $self->upper_case;
                my $place_holder = "$key $tag_name ?";
                $sql .= $place_holder;
            }
            elsif ($tag_name eq 'insert_values') {
                my $statement_keys          = '(';
                my $statement_place_holders = '(';
                
                $values = $values->{insert_values};
                
                foreach my $key (sort keys %$values) {
                    if ($bind_filter) {
                        push @bind_values, scalar $bind_filter->($key, $values->{$key});
                    }
                    else {
                        push @bind_values, $values->{$key};
                    }
                    
                    $statement_keys          .= "$key, ";
                    $statement_place_holders .= "?, ";
                }
                
                $statement_keys =~ s/, $//;
                $statement_keys .= ')';
                
                $statement_place_holders =~ s/, $//;
                $statement_place_holders .= ')';
                
                $sql .= "$statement_keys values $statement_place_holders";
            }
            elsif ($tag_name eq 'update_set') {
                my $statement          = 'set ';
                
                $values = $values->{update_set};
                
                foreach my $key (sort keys %$values) {
                    if ($bind_filter) {
                        push @bind_values, scalar $bind_filter->($key, $values->{$key});
                    }
                    else {
                        push @bind_values, $values->{$key};
                    }
                    
                    $statement          .= "$key = ?, ";
                }
                
                $statement =~ s/, $//;
                
                $sql .= $statement;
            }
        }
    }
    $sql .= ';' unless $sql =~ /;$/;
    return ($sql, @bind_values);
}

sub tag_processors : Attr {type => 'hash', deref => 1, auto_build => sub { 
    shift->tag_processors(
        '='    => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder,
        '<>'   => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder,
        '<'    => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder,
        '>='   => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder,
        '<='   => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder,
        'like' => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder,
        'in'   => \&DBI::Custom::SQL::Template::TagProcessor::expand_place_holder
    );
}}

sub add_tag_processor {
    
}

Object::Simple->build_class;


package DBI::Custom::SQL::Template::TagProcessor;

sub expand_place_holder {
    my ($tag_name, $args, $values, $bind_filter, $sql_tmpl_obj) = @_;
    
    my $key = $args->[0];
    
    my @bind_values;
    # Filter Value
    if ($tag_name eq 'in') {
        $values->{$key} = [$values->{$key}] unless ref $values->{$key} eq 'ARRAY';
        if ($bind_filter) {
            for (my $i = 0; $i < @$values; $i++) {
                push @bind_values, scalar $bind_filter->($key, $values->{$key}->[$i]);
            }
        }
        else {
            for (my $i = 0; $i < @$values; $i++) {
                push @bind_values, $values->{$key}->[$i];
            }
        }
    }
    else {
        if ($bind_filter) {
            push @bind_values, scalar $bind_filter->($key, $values->{$key});
        }
        else {
            push @bind_values, $values->{$key};
        }
    }
    if ($bind_filter) {
        if ($tag_name eq 'in') {
            for (my $i = 0; $i < @$values; $i++) {
                push @bind_values, scalar $bind_filter->($key, $values->{$key}->[$i]);
            }
        }
        else {
            push @bind_values, scalar $bind_filter->($key, $values->{$key});
        }
    }
    else {
        push @bind_values, $values->{$key};
    }
    
    $tag_name = uc $tag_name if $sql_tmpl_obj->upper_case;
    
    my $expand;
    if ($tag_name eq '?') {
        $expand = '?';
    }
    elsif ($tag_name eq 'in') {
        $expand = '(';
        for (my $i = 0; $i < @$values; $i++) {
            $expand .= '?, ';
        }
        $expand =~ s/, $'//;
        $expand .= ')';
    }
    else {
        $expand = "$key $tag_name ?";
    }
    
    return ($expand, \@bind_values);
}


package DBI::Custom;
1;

=head1 NAME

DBI::Custom - Customizable simple DBI

=head1 VERSION

Version 0.0101

=cut

=head1 SYNOPSIS

  my $dbi = DBI::Custom->new;

=head1 METHODS

=head2 add_filter

    

=head2 bind_filter

=head2 clone

=head2 connect

=head2 connect_info

=head2 dbh

=head2 fetch_filter

=head2 filters

=head2 initialize_class

=head2 prototype

=head2 new

=head2 query

=head2 create_sql

=head2 query_raw_sql

=head2 sql_template

=head2 auto_commit

=head2 connected

=head2 dbh_option

=head2 disconnect

=head2 reconnect

=head2 result_class

=head2 commit

=head2 rollback


=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of DBI::Custom
