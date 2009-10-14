package DBI::Custom;
use Object::Simple;
use DBI;
use SQL::Abstract;

sub new {
    my $self = shift->Object::Simple::new(@_);
    my $class = ref $self;
    return bless {%{$class->model}, %{$self}}, $class;
}

sub create_model {shift->Object::Simple::new(@_);

sub initialize_model {
    my ($class, $callback) = @_;
    
    my $model = $class->create_model;
    
    $callback->($model);
    
    $class->model($model);
}

# Class attribute
sub connect_info : Attr { type => 'hash' }
sub table_infos  : Attr { type => 'hash' }
sub dbh          : Attr {}
sub sql_abstract : Attr { auto_build => sub { shift->sql_abstract(SQL::Abstract->new) }}

sub column_info {
    my ($self, $table, $column_name, $column_info) = @_;
    
    if (@_ > 3) {
        $self->table_infos->{$table}{column}{$column_name} = $column_info;
        return $self;
    }
    return $self->table_infos->{$table}{column}{$column_name};
}

sub columns {
    my ($self, $table) = @_;
    
    return sort { 
        $self->table_infos->{$table}{column}{$a}{pos} 
        <=>
        $self->table_infos->{$table}{column}{$b}{pos}
    } keys %{$self->table_info->{$table}{column}}
}

sub tables {
    my $self = shift;
    return keys %{$self->table_info};
}

sub create_table {
    my ($self, $table, @row_infos) = @_;
    
    $self->table_infos->{$table} = {};
    
    for (my $i = 0; $i < @columns; i++) {
        my $column = $columns[$i];
        
        my $column_name = shift @$column;
        my $column_type = shift @$column;
        my %column_options = @$column;
        
        my $column_info = {};
        
        $column_info->{pos}     = $i;
        $column_info->{type}    = $column_type;
        $column_info->{options} = \%column_options;
        
        $self->column_info($table, $column_name, $column_info);
    }
}

sub load_table_definitions {
    my $self = shift;
    my $dsn  = $self->connect_info->{dsn};
}

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
