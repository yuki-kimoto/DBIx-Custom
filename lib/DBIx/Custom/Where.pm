package DBIx::Custom::Where;
use Object::Simple -base;

use Carp 'croak';
use DBIx::Custom::Util '_subname';
use overload 'bool' => sub {1}, fallback => 1;
use overload '""' => sub { shift->to_string }, fallback => 1;

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

has [qw/dbi param/],
    clause => sub { [] },
    map_if => 'exists';

sub new {
    my $self = shift->SUPER::new(@_);
    
    # Check attribute names
    my @attrs = keys %$self;
    foreach my $attr (@attrs) {
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
        foreach my $column (keys %{$self->param}) {
            croak qq{"$column" is not safety column name (} . _subname . ")"
              unless $column =~ /^[$safety\.]+$/;
        }
    }
    # Clause
    my $clause = $self->clause;
    $clause = ['and', $clause] unless ref $clause eq 'ARRAY';
    $clause->[0] = 'and' unless @$clause;
    
    # Map condition
    my $map_if = $self->map_if || '';
    $map_if = $map_if eq 'exists' ? $map_if
            : $map_if eq 'defined' ? sub { defined $_[0] }
            : $map_if eq 'length'  ? sub { length $_[0] }
            : ref $map_if eq 'CODE' ? $map_if
            : undef;
    
    croak "You can must specify right value to C<map_if> " . _subname
      unless $map_if;
    $self->{_map_if} = $map_if;
    
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
        my $columns = $self->dbi->query_builder->build_query($clause)->columns;
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
        if (my $q = $self->dbi->_quote) {
            $q = quotemeta($q);
            $column =~ s/[$q]//g;
        }
        
        # Check safety
        my $safety = $self->dbi->safety_character;
        croak qq{"$column" is not safety column name (} . _subname . ")"
          unless $column =~ /^[$safety\.]+$/;
        
        # Column count up
        my $count = ++$count->{$column};
        
        # Push
        my $param = $self->param;
        if (ref $param eq 'HASH') {
            if (exists $param->{$column}) {
                my $map_if = $self->{_map_if};
                
                if (ref $param->{$column} eq 'ARRAY') {
                    unless (ref $param->{$column}->[$count - 1] eq 'DBIx::Custom::NotExists') {
                        if ($map_if eq 'exists') {
                            $pushed = 1 if exists $param->{$column}->[$count - 1];
                        }
                        else {
                            $pushed = 1 if $map_if->($param->{$column}->[$count - 1]);
                        }
                    }
                } 
                elsif ($count == 1) {
                    if ($map_if eq 'exists') {
                        $pushed = 1 if  exists $param->{$column};
                    }
                    else {
                        $pushed = 1 if $map_if->($param->{$column});
                    }
                }
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

=head2 C<map_if EXPERIMENTAL>
    
    my $map_if = $where->map_if($condition);
    $where->map_if($condition);

If C<clause> contain named placeholder like ':title{=}'
and C<param> contain the corresponding key like {title => 'Perl'},
C<to_string> method join the cluase and convert to placeholder
like 'title = ?'.

C<map_if> method can change this mapping rule.
Default is C<exists>. If the key exists, mapping is done.
    
    $where->map_if('exists');

In case C<defined> is specified, if the value is defined,
mapping is done.

    $where->map_if('defined');

In case C<length> is specified, the value is defined
and the length is bigger than 0, mappting is done.

    $where->map_if('length');

You can also subroutine like C<sub { defined $_[0] }> for mappging.

    $where->map_if(sub { defined $_[0] });

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
