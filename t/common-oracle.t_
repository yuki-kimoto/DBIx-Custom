use strict;
use warnings;

use FindBin;
$ENV{DBIX_CUSTOM_TEST_RUN} = 1
  if -f "$FindBin::Bin/run/common-oracle.run";
$ENV{DBIX_CUSTOM_SKIP_MESSAGE} = 'oracle private test';

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';

    my $date_typename = 'CHAR(10)';
    my $datetime_typename = 'DATE';

    sub date_typename { lc 'CHAR' }
    sub datetime_typename { lc $datetime_typename }

    my $date_datatype = 91;
    my $datetime_datatype = 11;

    sub date_datatype { lc $date_datatype }
    sub datetime_datatype { lc $datetime_datatype }
    
    has datetime_suffix => '';

    has dsn => 'dbi:Oracle:host=localhost;port=1521;sid=XE';
    has user  => 'dbix_custom';
    has password => 'dbix_custom';
    has exclude_table => sub {

        return qr/^(
            pg_|column_|role_|view_|sql_
            |applicable_roles
            |check_constraints
            |columns
            |constraint_column_usage
            |constraint_table_usage
            |data_type_privileges
            |domain_constraints
            |domain_udt_usage
            |domains
            |element_types
            |enabled_roles
            |information_schema
            |information_schema_catalog_name
            |key_column_usage
            |parameters
            |referential_constraints
            |routine_privileges
            |routines
            |schemata
            |table_constraints
            |table_privileges
            |tables
            |triggered_update_columns
            |triggers
            |usage_privileges
            |views
        )/x
    };
    
    sub create_table1 { 'create table table1 (key1 varchar2(255), key2 varchar2(255));' }
    sub create_table1_2 {'create table table1 (key1 varchar2(255), key2 varchar2(255), '
     . 'key3 varchar2(255), key4 varchar2(255), key5 varchar2(255));' }
    sub create_table1_type { "create table table1 (key1 $date_typename, key2 $datetime_typename);" }
    sub create_table1_highperformance { "create table table1 (ab varchar2(255), bc varchar2(255), "
      . "ik varchar2(255), hi varchar2(255), ui varchar2(255), pq varchar2(255), dc varchar2(255));" }
    sub create_table2 { 'create table table2 (key1 varchar2(255), key3 varchar2(255));' }
    sub create_table2_2 { "create table table2 (key1 varchar2(255), key2 varchar2(255), key3 varchar2(255))" }
    sub create_table3 { "create table table3 (key1 varchar2(255), key2 varchar2(255), key3 varchar2(255))" }
    sub create_table_reserved { 'create table "table" ("select" varchar2(255), "update" varchar2(255))' }
}

require "$FindBin::Bin/common.t";
