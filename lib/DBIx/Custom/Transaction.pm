package DBIx::Custom::Transaction;

use strict;
use warnings;

use base 'Object::Simple';
use Carp 'croak';

__PACKAGE__->attr('dbi');

sub run {
    my ($self, $transaction) = @_;
    
    # DBIx::Custom object
    my $dbi = $self->dbi;
    
    # Shorcut
    return unless $dbi;
    
    # Check auto commit
    croak("AutoCommit must be true before transaction start")
      unless $dbi->_auto_commit;
    
    # Auto commit off
    $dbi->_auto_commit(0);
    
    # Run transaction
    eval {$transaction->()};
    
    # Tranzaction error
    my $transaction_error = $@;
    
    # Tranzaction is failed.
    if ($transaction_error) {
        # Rollback
        eval{$dbi->dbh->rollback};
        
        # Rollback error
        my $rollback_error = $@;
        
        # Auto commit on
        $dbi->_auto_commit(1);
        
        if ($rollback_error) {
            # Rollback is failed
            croak("${transaction_error}Rollback is failed : $rollback_error");
        }
        else {
            # Rollback is success
            croak("${transaction_error}Rollback is success");
        }
    }
    # Tranzaction is success
    else {
        # Commit
        eval{$dbi->dbh->commit};
        my $commit_error = $@;
        
        # Auto commit on
        $dbi->_auto_commit(1);
        
        # Commit is failed
        croak($commit_error) if $commit_error;
    }
}

1;

=head1 NAME

DBIx::Custom::Transaction - Transaction

=head1 SYNOPSYS
    
    use DBIx::Custom::Transaction
    my $txn = DBIx::Custom::Transaction->new(dbi => DBIx::Custom->new);
    $txn->run(sub { ... });
    
=head1 ATTRIBUTES

=head2 dbi

    $self = $txn->dbi($dbi);
    $dbi  = $txn->dbi;
    
=head1 METHODS

=head2 run
    
    $txn->run(
        sub {
            # Transaction
        }
    );

=cut
