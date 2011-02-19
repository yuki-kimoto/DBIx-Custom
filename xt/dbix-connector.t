use Test::More 'no_plan';

{
     package MyDBI1;
     
     use base 'DBIx::Custom';
     
     use DBIx::Connector;
     
     __PACKAGE__->attr(connection_manager => sub {
         my $self = shift;
         
         my $cm = DBIx::Connector->new(
             $self->data_source,
             $self->user,
             $self->password,
             {
                 %{$self->default_dbi_option},
                 %{$self->dbi_option}
             }
         );
         
         return $cm
     });
     
     sub dbh { shift->connection_manager->dbh }
     
     sub connect {
         my $self = shift->SUPER::new(@_);
         
         return $self;
     }
}

# user password database
our ($USER, $PASSWORD, $DATABASE) = connect_info();

# Functions for tests
sub connect_info {
    my $file = 'password.tmp';
    open my $fh, '<', $file
      or return;
    
    my ($user, $password, $database) = split(/\s/, (<$fh>)[0]);
    
    close $fh;
    
    return ($user, $password, $database);
}

my $dbi = MyDBI1->connect(
    user => $USER, password => $PASSWORD,
    data_source => "dbi:mysql:database=$DATABASE");

$dbi->delete_all(table => 'table1');
$dbi->insert(table => 'table1', param => {key1 => 1, key2 => 2});
is_deeply($dbi->select(table => 'table1')->fetch_hash_all, [{key1 => 1, key2 => 2}]);






