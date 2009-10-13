package DBI::Custom;
use Object::Simple;

# Import
sub setup {
    my ($class, $setup) = @_;
    
    # Import function to caller class;
    $class->_import_functions_to($class);
    
    # Setup Caller class
    $setup->();
    
    # Remove function from caller class;
    $class->_remove_imported_functions($class);
}

# Tempature import functions
sub _temp_import_functions {
    connect_info => sub {
        my %options = @_;
        my $class = caller;
        $class->DBI::Custom::connect_info(%options);
    },
    create_table => sub {
        my $table = shift;
        my @row_infos = @_;
        
        my $class = caller;
        $class->table_infos->{$table} = {};
        
        for (my $i = 0; $i < @columns; i++) {
            my $column = $columns[$i];
            
            my $column_name = shift @$column;
            my $column_type = shift @$column;
            my %column_options = @$column;
            
            my $column_info
              = $class->table_infos->{$table}{column}{$column_name} = {};
            
            $column_info->{pos}     = $i;
            $column_info->{type}    = $column_type;
            $column_info->{options} = \%column_options;
        }
    }
}

# Import functions to caller class
sub _import_functions_to {
    my ($self, $class) = @_;
    no strict 'refs';
    foreach my $import_function (keys %{$self->_temp_import_functions}) {
        *{"${class}::$import_function"}
          = $self->_temp_import_functions->{$import_function};
    }
}

# Remove functions from caller class
sub _remove_imported_functions {
    my ($self, $class) = @_;
    no strict 'refs';
    foreach my $import_function (keys %{$self->_temp_import_functions}) {
        delete ${$class . '::'}{"$import_function"};    
    }
}


# Class attribute
sub connect_info : ClassAttr { type => 'hash', default => sub { {} } }
sub table_infos : ClassAttr { type => 'hash', default => sub { {} } }

sub column_info {
    my ($class, $table, $column_name) = @_;
    return $class->table_infos->{$table}{column}{$column_name};
}

sub columns {
    my ($class, $table) = @_;
    
    return sort { 
        $class->table_infos->{$table}{column}{$a}{pos} 
        <=>
        $class->table_infos->{$table}{column}{$b}{pos}
    } keys %{$class->table_info->{$table}{column}}
}

sub tables {
    my $class = shift;
    return keys %{$self->table_info};
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
