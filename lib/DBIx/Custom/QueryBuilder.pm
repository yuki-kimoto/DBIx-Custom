package DBIx::Custom::QueryBuilder;

use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Query;
use DBIx::Custom::Util '_subname';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;
push @DBIx::Custom::Where::CARP_NOT, __PACKAGE__;

sub build_query {
    my ($self, $sql) = @_;
    
    $sql ||= '';
    my $columns = [];
    my $c = ($self->{dbi} || {})->{safety_character}
      || $self->dbi->safety_character;
    # Parameter regex
    $sql =~ s/([0-9]):/$1\\:/g;
    while ($sql =~ /(^|.*?[^\\]):([$c\.]+)(?:\{(.*?)\})?(.*)/sg) {
        push @$columns, $2;
        $sql = defined $3 ? "$1$2 $3 ?$4" : "$1?$4";
    }
    $sql =~ s/\\:/:/g if index($sql, "\\:") != -1;

    # Create query
    bless {sql => $sql, columns => $columns}, 'DBIx::Custom::Query';
}

# DEPRECATED
has 'dbi';

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

DBIx::Custom::QueryBuilder - DEPRECATED!

=head1 DESCRIPTION

This module functionality will be moved to DBIx::Custom

=cut
