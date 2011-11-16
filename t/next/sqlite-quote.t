# Change quote for tests
use DBIx::Custom::Next;
{
    package DBIx::Custom::Next;
    no warnings 'redefine';
    sub quote { '""' }
}

use FindBin;

require "$FindBin::Bin/sqlite.t";
