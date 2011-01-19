package DBIx::Custom::Where;

use strict;
use warnings;

use base 'Object::Simple';

use overload 'bool' => sub {1}, fallback => 1;
use overload '""' => sub { shift->to_string }, fallback => 1;

use Carp 'croak';

__PACKAGE__->attr(clause => sub { [] });
__PACKAGE__->attr(param => sub { {} });
__PACKAGE__->attr(sql_builder => sub { {} });

sub to_string {
    my ($self, $param, $clause) = @_;
    
    local $self->{_where}    = '';
    local $self->{_count}    = {};
    local $self->{_op_stack} = [];
    local $self->{_param}    = $param;
    
    $clause = ['and', $clause] unless ref $clause eq 'ARRAY';
    
    $self->_forward($clause);
    
    return $self->{_where};
}

our %VALID_OPERATIONS = map { $_ => 1 } qw/and or or_repeat/;

sub _forward {
    my ($self, $clause) = @_;
    
    if (ref $clause eq 'ARRAY') {
        $self->{_where} .= '( ';
        
        my $op = $clause->[0] || '';
        
        croak qq{"$op" is invalid operation}
          unless $VALID_OPERATIONS{$op};
          
        push @{$self->{_op_stack}}, $op;
        
        for (my $i = 1; $i < @$clause; $i++) {
            $self->_forword($clause->[$i]);
        }
        
        pop @{$self->{_op_stack}};

        if ($self->{_where} =~ /\( $/) {
            $self->{_where} =~ s/\( $//;
            $self->{_where} .= ' ';
        }
        $self->{_where} =~ s/ $op $//;
        $self->{_where} .= ' ) ';
    }
    else {
        my $op = $self->{_op_stack}->[-1];
        
        my $columns = $self->sql_builder->build_query($clause)->columns;
        
        croak qq{each tag contains one column name: tag "$clause"}
          unless @$columns == 1;
        
        my $column = $columns->[0];
        
        my $ccount = ++$self->{_count}->{$column};
        
        my $param = $self->{_param};
        
        if (exists $param->{$column}) {
            if ($op eq 'and' || $op eq 'or') {
                if (ref $param->{$column} eq 'ARRAY') {
                    $self->{_where} .= $clause . " $op "
                      if exists $param->{$column}->[$ccount];
                }
                else {
                    $self->{_where} .= $clause . " $op "
                      if $ccount == 1;
                }
            }
            elsif ($op eq 'or_repeat') {
                if (ref $param->{$column} eq 'ARRAY') {
                    $self->{_where} .= $clause . " or "
                      for (1 .. @{$param->{$column}});
                }
                else {
                    $self->{_where} .= $clause;
                }
            }
        }
    }
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

=head2 C<to_string>

    $where->to_string;
