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

sub map {
    my ($self, %map) = @_;
    
    if ($self->if ne 'exists' || keys %map) {
        my $param = $self->_map_param($self->param, %map);
        $self->param($param);
    }
    return $self;
}

sub _map_param {
    my $self = shift;
    my $param = shift;
    
    return $param if !defined $param;
    
    my %map = @_;
    
    # Mapping
    my $map_param = {};
    foreach my $key (keys %$param) {
    
        my $value_cb;
        my $condition;
        my $map_key;
        
        # Get mapping information
        if (ref $map{$key} eq 'ARRAY') {
            foreach my $some (@{$map{$key}}) {
                $map_key = $some unless ref $some;
                $condition = $some->{if} if ref $some eq 'HASH';
                $value_cb = $some if ref $some eq 'CODE';
            }
        }
        elsif (defined $map{$key}) {
            $map_key = $map{$key};
        }
        else {
            $map_key = $key;
        }
        
        $value_cb ||= sub { $_[0] };
        $condition ||= $self->if || 'exists';

        # Map parameter
        my $value;
        if (ref $condition eq 'CODE') {
            if (ref $param->{$key} eq 'ARRAY') {
                $map_param->{$map_key} = [];
                for (my $i = 0; $i < @{$param->{$key}}; $i++) {
                    $map_param->{$map_key}->[$i]
                      = $condition->($param->{$key}->[$i]) ? $param->{$key}->[$i]
                      : $self->dbi->not_exists;
                }
            }
            else {
                $map_param->{$map_key} = $value_cb->($param->{$key})
                  if $condition->($param->{$key});
            }
        }
        elsif ($condition eq 'exists') {
            if (ref $param->{$key} eq 'ARRAY') {
                $map_param->{$map_key} = [];
                for (my $i = 0; $i < @{$param->{$key}}; $i++) {
                    $map_param->{$map_key}->[$i]
                      = exists $param->{$key}->[$i] ? $param->{$key}->[$i]
                      : $self->dbi->not_exists;
                }
            }
            else {
                $map_param->{$map_key} = $value_cb->($param->{$key})
                  if exists $param->{$key};
            }
        }
        else { croak qq/Condition must be code reference or "exists" / . _subname }
    }
    
    return $map_param;
}

sub if {
    my $self = shift;
    if (@_) {
        my $if = $_[0];
        
        $if = $if eq 'exists' ? $if
                : $if eq 'defined' ? sub { defined $_[0] }
                : $if eq 'length'  ? sub { defined $_[0] && length $_[0] }
                : ref $if eq 'CODE' ? $if
                : undef;

        croak "You can must specify right value to C<if> " . _subname
          unless $if;

        $self->{if} = $if;
        return $self;
    }
    $self->{if} = 'exists' unless exists $self->{if};
    return $self->{if};
}

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
    my $if = $self->if || '';
    $if = $if eq 'exists' ? $if
            : $if eq 'defined' ? sub { defined $_[0] }
            : $if eq 'length'  ? sub { length $_[0] }
            : ref $if eq 'CODE' ? $if
            : undef;
    
    croak "You can must specify right value to C<if> " . _subname
      unless $if;
    $self->{_if} = $if;
    
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

=head2 C<if EXPERIMENTAL>
    
    my $if = $where->if($condition);
    $where->if($condition);

C<if> is default of C<map> method C<if> option.

=head2 C<map EXPERIMENTAL>

Mapping parameter key and value. C<param> is converted,
so this method must be called after C<param> is set.

    $where->map(
        'id' => 'book.id',
        'author' => ['book.author' => sub { '%' . $_[0] . '%' }],
        'price' => [
            'book.price', {if => sub { length $_[0] }
        ]
    );

The following option is available.

=over 4

=item * C<if>

By default, if parameter key is exists, mapping is done.
    
    if => 'exists';

In case C<defined> is specified, if the value is defined,
mapping is done.

    if => 'defined';

In case C<length> is specified, the value is defined
and the length is bigger than 0, mappting is done.

    if => 'length';

You can also subroutine like C<sub { defined $_[0] }> for mappging.

    if => sub { defined $_[0] }

=back

=head2 C<to_string>

    $where->to_string;

Convert where clause to string.

double quote is override to execute C<to_string> method.

    my $string_where = "$where";

=cut
