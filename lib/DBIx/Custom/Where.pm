package DBIx::Custom::Where;

use strict;
use warnings;

use base 'Object::Simple';

use overload 'bool' => sub {1}, fallback => 1;
use overload '""' => sub { shift->to_string }, fallback => 1;

use Carp 'croak';

__PACKAGE__->attr(param => sub { {} });

sub clause {
    my $self = shift;
    
    if (@_) {
        $self->{clause} = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        return $self;
    }
    return $self->{clause} ||= {};
}

sub or_clause {
    my $self = shift;
    
    if (@_) {
        $self->{or_clause} = ref $_[0] eq 'HASH' ? $_[0] : {@_};
        
        return $self;
    }
    
    return $self->{or_clause} ||= {};
}

sub to_string {
    my $self = shift;
    
    my $param      = $self->param;
    my $clauses    = $self->clause;
    my $or_clauses = $self->or_clause;
    
    # Clause check
    my $wexists = keys %$param;
    
    # Where
    my $where = '';
    if ($wexists) {
        $where .= 'where (';
        
        foreach my $column (keys %$param) {

            croak qq{"$column" is not found in "clause" or "or_clause"}
              if exists $clauses->{$column}
                  && exists $or_clauses->{$column};
            
            if (exists $clauses->{$column}) {
                if (ref $clauses->{$column} eq 'ARRAY') {
                    foreach my $clause (@{$clauses->{$column}}) {
                        $where .= $clause . ' and ';
                    }
                }
                else {
                    $where .= $clauses->{$column} . ' and ';
                }
                            }
            elsif (exists $or_clauses->{$column}) {
                my $clause = $or_clauses->{$column};
                
                if (ref $param->{$column} eq 'ARRAY') {
                    my $count = @{$param->{$column}};
                    if ($count) {
                        $where .= '( ';
                        $where .= $clause . ' or ' for (1 .. $count);
                        $where =~ s/ or $//;
                        $where .= ' ) and ';
                    }
                }
                else {
                    $where .= $clause . ' and ';
                }
            }
        }

        $where =~ s/ and $//;
        $where .= ' )';
    }
    
    return $where;
}

1;

=head1 NAME

DBIx::Custom::Where - Where clause

=head1 SYNOPSYS

    $where = DBIx::Custom::Where->new;
    
    my $sql = "select * from book $where";

=head1 ATTRIBUTES

=head2 C<param>

    my $param = $where->param;
    $where    = $where->param({title => 'Perl',
                               date => ['2010-11-11', '2011-03-05']},
                               name => ['Ken', 'Taro']);
=head1 METHODS

=head2 C<clause>

    $where->clause(title => '{= title}', date => ['{< date}', '{> date}']);

Where clause. These clauses is joined by ' and ' at C<to_string()>
if corresponding parameter name is exists in C<param>.

=head2 C<or_clause>

    $where->or_clause(name => '{= name}');

clause which has these parameter name is joined by ' or '.

=head2 C<to_string>

    $where->to_string;
