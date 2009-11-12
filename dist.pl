use strict;
use warnings;
use File::Spec;

my @modules = qw/DBIx-Custom DBIx-Custom-Basic DBIx-Custom-MySQL
                 DBIx-Custom-Query DBIx-Custom-Result
                 DBIx-Custom-Result DBIx-Custom-SQLite
                 DBIx-Custom-SQL-Template/;

foreach my $module (@modules) {
    chdir $module
      or die "Cannot change directory '$module': $!";
    
    system('perl Build realclean');
    system('perl Build.PL');
    system('perl Build');
    system('perl Build test');
    system('perl Build install');
    system('perl Build disttest');
    system('perl Build dist');
    
    chdir File::Spec->updir
      or die "Cannot change up directory: $!";
}