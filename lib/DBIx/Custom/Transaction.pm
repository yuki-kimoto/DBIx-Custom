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

DBIx::Custom::TransactionScope - Transaction scope

=head1 SYNOPSYS

    use DBIx::Custom::SQLite;
    
    # New
    my $dbi = DBIx::Custom::SQLite->new(user => 'taro', $password => 'kliej&@K',
                                        database => 'sample');
    
    # Connect memory database
    my $dbi->connect_memory;

=head1 ATTRIBUTES

=head2 dbi

=head1 METHODS

=head2 run

=head1 AUTHOR

Yuki Kimoto, C<< <kimoto.yuki at gmail.com> >>

Github L<http://github.com/yuki-kimoto>

I develope this module L<http://github.com/yuki-kimoto/DBIx-Custom>

=head1 Copyright & lisence

Copyright 2009 Yuki Kimoto, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

