package DBIx::Custom::Where;

use strict;
use warnings;

use base 'Object::Simple';

use overload 'bool' => sub {1}, fallback => 1;
use overload '""' => sub { shift->to_string }, fallback => 1;

use Carp 'croak';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

__PACKAGE__->attr(
  [qw/param query_builder/],
  clause => sub { [] },
);

sub to_string {
    my $self = shift;
    
    # Clause
    my $clause = $self->clause;
    $clause = ['and', $clause] unless ref $clause eq 'ARRAY';
    $clause->[0] = 'and' unless @$clause;

    # Parse
    my $where = [];
    my $count = {};
    $self->_parse($clause, $where, $count, 'and');
    
    # Stringify
    unshift @$where, 'where' if @$where;
    return join(' ', @$where);
}

our %VALID_OPERATIONS = map { $_ => 1 } qw/and or/;

sub _parse {
    my ($self, $clause, $where, $count, $op) = @_;
    
    # Array
    if (ref $clause eq 'ARRAY') {
        
        # Start
        push @$where, '(';
        
        # Operation
        my $op = $clause->[0] || '';
        croak qq{"$op" is invalid operation}
          unless $VALID_OPERATIONS{$op};
        
        # Parse internal clause
        for (my $i = 1; $i < @$clause; $i++) {
            my $pushed = $self->_parse($clause->[$i], $where, $count, $op);
            push @$where, $op if $pushed;
        }
        pop @$where if $where->[-1] eq $op;
        
        # Undo
        if ($where->[-1] eq '(') {
            pop @$where;
            pop @$where;
        }
        # End
        else { push @$where, ')' }
    }
    
    # String
    else {
        
        # Column
        my $columns = $self->query_builder->build_query($clause)->columns;
        croak qq{each tag contains one column name: tag "$clause"}
          unless @$columns == 1;
        my $column = $columns->[0];
        
        # Column count up
        my $count = ++$count->{$column};
        
        # Push
        my $param = $self->param;
        my $pushed;
        if (defined $param) {
            if (exists $param->{$column}) {
                if (ref $param->{$column} eq 'ARRAY') {
                    $pushed = 1 if exists $param->{$column}->[$count - 1];
                }
                elsif ($count == 1) {
                    $pushed = 1;
                }
            }
            push @$where, $clause if $pushed;
        }
        else {
            push @$where, $clause;
            $pushed = 1;
        }
        
        return $pushed;
    }
}

1;

=head1 NAME

DBIx::Custom::Where - Where clause

=head1 SYNOPSYS

    my $where = DBIx::Custom::Where->new;

=head1 ATTRIBUTES

=head2 C<param>

    my $param = $where->param;
    $where    = $where->param({title => 'Perl',
                               date => ['2010-11-11', '2011-03-05']},
                               name => ['Ken', 'Taro']);
=head1 METHODS

=head2 C<clause>

    $where->clause(
        ['and', '{= title}', ['or', '{< date}', '{> date}']]
    );

Where clause. Above one is expanded to the following SQL by to_string
If all parameter names is exists.

    "where ( {= title} and ( {< date} or {> date} ) )"

=head2 C<to_string>

    $where->to_string;

Convert where clause to string correspoinding to param name.

