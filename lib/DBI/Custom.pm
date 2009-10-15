package DBI::Custom;
use Object::Simple;
use DBI;
use SQL::Abstract;

# Model
sub model : ClassAttr { auto_build => \&_inherit_model }

# Inherit super class model
sub _inherit_model {
    $class = shict;
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
    $new->connect_infos(%{$self->connect_infos || {}});
    $new->filters(%{$self->filters || {}});
    
    $new->global_bind_rules(@{$self->global_bind_rules || []});
    $new->global_fetch_rules(@{$self->global_fetch_rules || []});
    
    foreach my $method (qw/bind_rules fetch_rules/) {
        my $new_rules = [];
        foreach my $rule (@{$self->method}) {
            my $new_rule = {};
            foreach my $key ($rule) {
                if ($key eq 'filter') {
                    my $new_filters = [];
                    foreach my $filter (@{$rule->{$key} || []}) {
                        push @$new_filters, $filter
                    }
                    $new_rule->{$key} = $new_filters;
                }
                else {
                     $new_rule->{$key} = $rule->{$key};
                }
            }
            push @$new_rules, $new_rule;
        }
        $self->$method($new_rules);
    }
}

# Attribute
sub connect_info       : Attr { type => 'hash',  auto_build => sub { shift->connect_info({}) } }

sub global_bind_rules  : Attr { type => 'array', auto_build => sub { shift->global_bind_rules([]) } }
sub add_global_bind_rule { shift->global_bind_rules(@_) }

sub global_fetch_rules : Attr { type => 'array', auto_build => sub { shift->global_fetch_rules([]) } }
sub add_global_fetch_rules { shift->global_fetch_rules(@_) }

sub bind_rules : Attr { type => 'hash',  auto_build => sub { shift->bind_rules({}) }
sub add_bind_rule { shift->bind_rules(@_) }

sub fetch_rules : Attr { type => 'hash',  auto_build => sub { shift->fetch_rules({}) }
sub add_fetch_rule { shift->fetch_rules(@_) }

sub filters : Attr { type => 'hash', deref => 1, default => sub { {} } }
sub add_filter { shift->filters(@_) }


sub dbh          : Attr { auto_build => sub { shift->connect } }
sub sql_abstract : Attr { auto_build => sub { shift->sql_abstract(SQL::Abstract->new) }}

sub connect {
    my $self = shift;
    my $connect_info = $self->connect_info;
    
    my $dbh = DBI->connect(
        $connect_info->{dsn},
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
}

sub reconnect {
    my $self = shift;
    $self->dbh(undef);
    $self->connect;
}

sub query {
    my ($self, $query, @binds) = @_;
    $self->{success} = 0;

    $self->_replace_omniholder(\$query, \@binds);

    my $st;
    my $sth;

    my $old = $old_statements{$self};

    if (my $i = (grep $old->[$_][0] eq $query, 0..$#$old)[0]) {
        $st = splice(@$old, $i, 1)->[1];
        $sth = $st->{sth};
    } else {
        eval { $sth = $self->{dbh}->prepare($query) } or do {
            if ($@) {
                $@ =~ s/ at \S+ line \d+\.\n\z//;
                Carp::croak($@);
            }
            $self->{reason} = "Prepare failed ($DBI::errstr)";
            return _dummy;
        };

        # $self is quoted on purpose, to pass along the stringified version,
        # and avoid increasing reference count.
        $st = bless {
            db    => "$self",
            sth   => $sth,
            query => $query
        }, 'DBIx::Simple::Statement';
        $statements{$self}{$st} = $st;
    }

    eval { $sth->execute(@binds) } or do {
        if ($@) {
            $@ =~ s/ at \S+ line \d+\.\n\z//;
            Carp::croak($@);
        }

        $self->{reason} = "Execute failed ($DBI::errstr)";
	return _dummy;
    };

    $self->{success} = 1;

    return bless { st => $st, lc_columns => $self->{lc_columns} }, $self->{result_class};
}

sub query {
    my ($self, $sql) = @_;
    my $sth = $self->dbh->prepare($sql);
    $sth->execute(@bind);
}

sub select {
    my ($table, $column_names, $where, $order) = @_;
    
    my ($stmt, @bind) = $self->sql_abstract->select($table, $column_names, $where, $order);
    my $sth = $self->dbh->prepare($stmt);
    $sth->execute(@bind);
}

sub insert {
    my ($self, $table, $values) = @_;
    
    my ($stmt, @bind) = $self->sql_abstract->insert($table, $values);
    my $sth = $self->dbh->prepare($stmt);
    $sth->execute(@bind);
}

sub update {
    my ($self, $values, $where) = @_;
    my ($stmt, @bind) = $self->sql_abstract->update($table, $values, $where);
    my $sth = $self->dbh->prepare($stmt);
    $sth->execute(@bind);
}

sub delete {
    my ($self, $where) = @_;
    my ($stmt, @bind) = $self->sql_abstract->delete($table, $where);
    my $sth = $self->dbh->prepare($stmt);
    $sth->execute(@bind);
}



Object::Simple->build_class;

=head1 NAME

DBI::Custom - The great new DBI::Custom!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use DBI::Custom;

    my $foo = DBI::Custom->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 FUNCTIONS

=head2 function1

=cut

sub function1 {
}

=head2 function2

=cut

sub function2 {
}

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
