package DBI::Custom;
use Object::Simple;

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






sub insert {
    my $self = shift;
    
    
    
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
