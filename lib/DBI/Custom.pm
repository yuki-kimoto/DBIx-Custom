package DBI::Custom;
use Object::Simple;

our $VERSION = '0.0101';

use Carp 'croak';
use DBI;

# Model
sub model : ClassAttr { auto_build => \&_inherit_model }

# Inherit super class model
sub _inherit_model {
    my $class = shift;
    my $super = do {
        no strict 'refs';
        ${"${class}::ISA"}[0];
    };
    my $model = eval{$super->can('model')}
                         ? $super->model->clone
                         : $class->Object::Simple::new;
    
    $class->model($model);
}

# New
sub new {
    my $self = shift->Object::Simple::new(@_);
    my $class = ref $self;
    return bless {%{$class->model->clone}, %{$self}}, $class;
}

# Initialize modle
sub initialize_model {
    my ($class, $callback) = @_;
    
    # Callback to initialize model
    $callback->($class->model);
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
sub dbh          : Attr { auto_build => sub { shift->connect } }
sub sql_template : Attr { auto_build => sub { shift->sql_template(DBI::Custom::SQLTemplate->new) } }


our %VALID_CONNECT_INFO = map {$_ => 1} qw/data_source user password options/;

# Connect
sub connect {
    my $self = shift;
    my $connect_info = $self->connect_info;
    
    foreach my $key (keys %{$self->connect_info}) {
        croak("connect_info '$key' is invald")
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
    
    $self->auto_commit($dbh->{AutoCommit});
    $self->dbh($dbh);
}

sub DESTROY {
    my $self = shift;
    $self->disconnect;
}

# Is connected?
sub connected {
    my $self = shift;
    return exists $self->{dbh} && eval {$self->dbh->can('prepare')};
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


sub create_sql {
    my $self = shift;
    
    my ($sql, @bind) = $self->sql_template->create_sql(@_);
    
    return ($sql, @bind);
}

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
    my $sth = $self->dbh->prepare($sql);
    
    if ($sth_options) {
        foreach my $key (keys %$sth_options) {
            $sth->{$key} = $sth_options->{$key};
        }
    }
    
    $sth->execute(@bind);
    
    # Select
    if ($sth->{NUM_OF_FIELDS}) {
        my $result_class = $self->result_class;
        my $result = $result_class->new({sth => $sth});
        return $result;
    }
    return;
}


sub query_raw_sql {
    my ($self, $sql, @bind) = @_;
    my $sth = $self->dbh->prepare($sql);
    $sth->execute(@bind);
    return $sth;
}

sub auto_commit : Attr {}


Object::Simple->build_class;

package DBI::Custom::Result;
use Object::Simple;

sub sth : Attr {};

sub fetchrow_arrayref {
    my $self = shift;
    $self->sth;
    
    
}


*fetch = \&fetchrow_arrayref;

sub err    { shift->sth->err }
sub errstr { shift->sth->errstr }
sub finish { shift->sth->finish }
sub rows   { shift->sth->rows }
sub state  { shift->sth->state }

Object::Simple->build_class;


package DBI::Custom::SQLTemplate;
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
            croak("Tag '$tag' in SQL template is invalid.\n\n" .
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


Object::Simple->build_class;

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

=head2 initialize_model

=head2 model

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

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbi-custom at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBI-Custom>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBI::Custom


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBI-Custom>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBI-Custom>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBI-Custom>

=item * Search CPAN

L<http://search.cpan.org/dist/DBI-Custom/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of DBI::Custom
