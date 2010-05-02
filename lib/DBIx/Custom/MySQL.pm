package DBIx::Custom::MySQL;

use warnings;
use strict;

use base 'DBIx::Custom::Basic';
use Carp 'croak';

__PACKAGE__->register_format(
    datetime => __PACKAGE__->formats->{SQL99_datetime},
    date     => __PACKAGE__->formats->{SQL99_date},
    time     => __PACKAGE__->formats->{SQL99_time},
);

sub connect {
    my $self = shift;
    
    # Create data source
    if (!$self->data_source) {
        my $database = $self->database;
        my $host     = $self->host;
        my $port     = $self->port;
        my $data_source = "dbi:mysql:";
        my $data_source_original = $data_source;
        $data_source .= "database=$database;" if $database;
        $data_source .= "host=$host;"         if $host;
        $data_source .= "port=$port;"         if $port;
        $data_source =~ s/:$// if $data_source eq $data_source_original;
        $self->data_source($data_source);
    }
    
    return $self->SUPER::connect;
}

sub last_insert_id {
    my $self = shift;
    
    # Not connected
    croak "Not yet connected" unless $self->connected;
    
    # Get last insert id
    my $last_insert_id = $self->dbh->{mysql_insertid};
    
    return $last_insert_id;
}

=head1 NAME

DBIx::Custom::MySQL - DBIx::Custom MySQL implementation

=head1 SYNOPSYS

    # New
    my $dbi = DBIx::Custom::MySQL->new(user => 'taro', $password => 'kliej&@K',
                                       database => 'sample_db');

=head1 METHODS

This class is L<DBIx::Custom::Basic> subclass.
You can use all methods of L<DBIx::Custom::Basic>

=head2 connect

Connect to database

    $self->connect;

If you set database, host, or port, data source is automatically created.

=head2 last_insert_id

    $last_insert_id = $dbi->last_insert_id;

The folloing is last_insert_id sample.

    $dbi->insert('books', {title => 'Perl', author => 'taro'});
    $last_insert_id = $dbi->last_insert_id;

This is equal to MySQL function

    last_insert_id()
    
=cut
