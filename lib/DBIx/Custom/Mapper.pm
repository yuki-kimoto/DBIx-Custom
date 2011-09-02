package DBIx::Custom::Mapper;
use Object::Simple -base;

use DBIx::Custom::NotExists;

use Carp 'croak';
use DBIx::Custom::Util '_subname';

# Carp trust relationship
push @DBIx::Custom::CARP_NOT, __PACKAGE__;

has [qw/param/],
    condition => sub {
        sub { defined $_[0] && length $_[0] }
    },
    pass => sub { [] };

sub map {
    my ($self, %rule) = @_;
    my $param = $self->param;
    $rule{$_} = $rule{$_} for @{$self->pass};
    
    # Mapping
    my $new_param = {};
    foreach my $key (keys %rule) {
        
        my $value_cb;
        my $condition;
        my $new_key;
        
        # Get mapping information
        $rule{$key} = [$rule{$key}] if ref $rule{$key} ne 'ARRAY';
        foreach my $some (@{$rule{$key}}) {
            $new_key = $some unless ref $some;
            $condition = $some->{condition} if ref $some eq 'HASH';
            $value_cb = $some if ref $some eq 'CODE';
        }
        $new_key = $key unless defined $new_key;
        $value_cb ||= sub { $_[0] };
        $condition ||= $self->condition;
        $condition = $self->_condition_to_sub($condition);

        # Map parameter
        my $value;
        if (ref $condition eq 'CODE') {
            if (ref $param->{$key} eq 'ARRAY') {
                $new_param->{$new_key} = [];
                for (my $i = 0; $i < @{$param->{$key}}; $i++) {
                    $new_param->{$new_key}->[$i]
                      = $condition->($param->{$key}->[$i]) ? $param->{$key}->[$i]
                      : DBIx::Custom::NotExists->singleton;
                }
            }
            else {
                $new_param->{$new_key} = $value_cb->($param->{$key})
                  if $condition->($param->{$key});
            }
        }
        elsif ($condition eq 'exists') {
            if (ref $param->{$key} eq 'ARRAY') {
                $new_param->{$new_key} = [];
                for (my $i = 0; $i < @{$param->{$key}}; $i++) {
                    $new_param->{$new_key}->[$i]
                      = exists $param->{$key}->[$i] ? $param->{$key}->[$i]
                      : DBIx::Custom::NotExists->singleton;
                }
            }
            else {
                $new_param->{$new_key} = $value_cb->($param->{$key})
                  if exists $param->{$key};
            }
        }
        else { croak qq/Condition must be code reference or "exists" / . _subname }
    }
    
    return $new_param;
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


sub _condition_to_sub {
    my ($self, $if) = @_;
    $if = $if eq 'exists' ? $if
            : $if eq 'defined' ? sub { defined $_[0] }
            : $if eq 'length'  ? sub { defined $_[0] && length $_[0] }
            : ref $if eq 'CODE' ? $if
            : undef;

    croak "You can must specify right value to C<condition> " . _subname
      unless $if;
    
    return $if;
}

1;

=head1 NAME

DBIx::Custom::Mapper - Mapper of parameter EXPERIMENTAL

=head1 SYNOPSYS

    my $mapper = $dbi->mapper(param => $param);
    my $new_param = $mapper->map(
        title => 'book.title', # Key
        author => sub { '%' . $_[0] . '%'} # Value
        price => ['book.price' => sub { '%' . $_[0] . '%' }], # Key and value
    );

=head1 ATTRIBUTES

=head2 C<param>

    my $param = $mapper->param;
    $mapper = $mapper->param({title => 'Perl', author => 'Ken'});

Parameter.

=head2 C<pass>

    my $pass = $mapper->pass;
    $mapper = $mapper->pass([qw/title author/]);

the key and value is copied without change when C<map> method is executed.

=head2 C<condition>

    my $condition = $mapper->condition;
    $mapper = $mapper->condition('exists');

Mapping condtion, default to C<length>.

You can set the following values to C<condition>.

=over 4

=item * C<exists>
   
    condition => 'exists'

If key exists, key and value is mapped.

=item * C<defined>

    condition => 'defined';

If value is defined, key and value is mapped.

=item * C<length>

    condition => 'length';

If value is defined and has length, key and value is mapped.

=item * C<code reference>

    condition => sub { defined $_[0] }

You can set code reference to C<condtion>.
The subroutine return true, key and value is mapped.

=head1 METHODS

L<DBIx::Custom::Mapper> inherits all methods from L<Object::Simple>
and implements the following new ones.

=head2 C<map>

    my $new_param = $mapper->map(
        price => 'book.price', # Key
        title => sub { '%' . $_[0] . '%'}, # Value
        author => ['book.author', sub { '%' . $_[0] . '%'}] # Key and value
    );

Map C<param>'s key and value and return new parameter.

For example, if C<param> is set to

    {
        price => 1900,
        title => 'Perl',
        author => 'Ken',
        issue_date => '2010-11-11'
    }

The following hash reference is returned.

    {
        'book.price' => 1900,
        title => '%Perl%',
        'book.author' => '%Ken%',
    }

By default, If the value has length, key and value is mapped.

    title => 'Perl'  # Mapped
    {title => '' }   # Not mapped
    {title => undef} # Not mapped
    {}               # Not mapped

You can set change mapping condition by C<condition> attribute.

    $mapper->condition('defined');

Or you can set C<condtion> option for each key.

    my $new_param = $mapper->map(
        price => ['book.price', {condition => 'defined'}]
        title => [sub { '%' . $_[0] . '%'}, {condition => 'defined'}] # Value
        author => ['book.author', sub { '%' . $_[0] . '%'}, condtion => 'exists']
    );

If C<pass> attrivute is set, the keys and value is copied without change.

    $mapper->pass([qw/title author/]);
    my $new_param = $mapper->map(price => 'book.price');

The following hash reference
    
    {title => 'Perl', author => 'Ken', price => 1900}

is mapped to

    {title => 'Perl', author => 'Ken', 'book.price' => 1900}

=cut
