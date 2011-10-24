package DBIx::Custom::Where;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util '_subname';
use overload 'bool' => sub {1}, fallback => 1;
use overload '""' => sub { shift->to_string }, fallback => 1;

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

has [qw/dbi param/],
    clause => sub { [] };

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attribute names
    my @attrs = keys %$self;
    for my $attr (@attrs) {
        croak qq{"$attr" is invalid attribute name (} . _subname . ")"
          unless $self->can($attr);
    }
    
    return $self;
}

sub to_string {
    my $self = shift;
    
    # Check if column name is safety character;
    my $safety = $self->dbi->safety_character;
    if (ref $self->param eq 'HASH') {
        for my $column (keys %{$self->param}) {
            croak qq{"$column" is not safety column name (} . _subname . ")"
              unless $column =~ /^[$safety\.]+$/;
        }
    }
    # Clause
    my $clause = $self->clause;
    $clause = ['and', $clause] unless ref $clause eq 'ARRAY';
    $clause->[0] = 'and' unless @$clause;
    
    # Parse
    my $where = [];
    my $count = {};
    $self->{_query_builder} = $self->dbi->query_builder;
    $self->{_safety_character} = $self->dbi->safety_character;
    $self->{_quote} = $self->dbi->_quote;
    $self->_parse($clause, $where, $count, 'and');

        
    # Check safety
    unless (join('', keys %$count) =~ /^[$self->{_safety_character}\.]+$/) {
        for my $column (keys %$count) {
            croak qq{"$column" is not safety column name (} . _subname . ")"
              unless $column =~ /^[$self->{_safety_character}\.]+$/;
        }
    }

    # Stringify
    unshift @$where, 'where' if @$where;
    return join(' ', @$where);
}

our %VALID_OPERATIONS = map { $_ => 1 } qw/and or/;
sub _parse {
    my ($self, $clause, $where, $count, $op, $info) = @_;
    
    # Array
    if (ref $clause eq 'ARRAY') {
        
        # Start
        push @$where, '(';
        
        # Operation
        my $op = $clause->[0] || '';
        croak qq{First argument must be "and" or "or" in where clause } .
              qq{"$op" is passed} . _subname . ")"
          unless $VALID_OPERATIONS{$op};
        
        my $pushed_array;
        # Parse internal clause
        for (my $i = 1; $i < @$clause; $i++) {
            my $pushed = $self->_parse($clause->[$i], $where, $count, $op);
            push @$where, $op if $pushed;
            $pushed_array = 1 if $pushed;
        }
        pop @$where if $where->[-1] eq $op;
        
        # Undo
        if ($where->[-1] eq '(') {
            pop @$where;
            pop @$where if ($where->[-1] || '') eq $op;
        }
        # End
        else { push @$where, ')' }
        
        return $pushed_array;
    }
    
    # String
    else {
        # Pushed
        my $pushed;
        
        # Column
        my $columns = $self->{_query_builder}->build_query($clause)->{columns};
        if (@$columns == 0) {
            push @$where, $clause;
            $pushed = 1;
            return $pushed;
        }
        elsif (@$columns != 1) {
            croak qq{Each part contains one column name: "$clause" (}
                  . _subname . ")";
        }
        
        # Remove quote
        my $column = $columns->[0];
        if (my $q = $self->{_quote}) {
            $q = quotemeta($q);
            $column =~ s/[$q]//g;
        }
        
        # Column count up
        my $count = ++$count->{$column};
        
        # Push
        my $param = $self->{param};
        if (ref $param eq 'HASH') {
            if (exists $param->{$column}) {
                my $if = $self->{_if};
                
                if (ref $param->{$column} eq 'ARRAY') {
                    $pushed = 1 if exists $param->{$column}->[$count - 1]
                      && ref $param->{$column}->[$count - 1] ne 'DBIx::Custom::NotExists'
                }
                elsif ($count == 1) { $pushed = 1 }
            }
            push @$where, $clause if $pushed;
        }
        elsif (!defined $param) {
            push @$where, $clause;
            $pushed = 1;
        }
        else {
            croak "Parameter must be hash reference or undfined value ("
                . _subname . ")"
        }
        return $pushed;
    }
    return;
}

1;

=head1 NAME

DBIx::Custom::Where - Where clause

=head1 SYNOPSYS

    my $where = DBIx::Custom::Where->new;
    my $string_where = "$where";

=head1 ATTRIBUTES

=head2 C<clause>

    my $clause = $where->clause;
    $where = $where->clause(
        ['and',
            'title = :title', 
            ['or', 'date < :date', 'date > :date']
        ]
    );

Where clause. Above one is expanded to the following SQL by to_string
If all parameter names is exists.

    "where ( title = :title and ( date < :date or date > :date ) )"

=head2 C<param>

    my $param = $where->param;
    $where = $where->param({
        title => 'Perl',
        date => ['2010-11-11', '2011-03-05'],
    });

=head2 C<dbi>

    my $dbi = $where->dbi;
    $where = $where->dbi($dbi);

L<DBIx::Custom> object.

=head1 METHODS

L<DBIx::Custom::Where> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<to_string>

    $where->to_string;

Convert where clause to string.

double quote is override to execute C<to_string> method.

    my $string_where = "$where";

=cut
