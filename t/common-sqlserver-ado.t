use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/common";
$ENV{DBIX_CUSTOM_TEST_RUN} = 1
  if -f "$FindBin::Bin/run/common-sqlserver.run";
$ENV{DBIX_CUSTOM_SKIP_MESSAGE} = 'sqlserver private test';

use DBIx::Custom;
{
    package DBIx::Custom;
    no warnings 'redefine';

    my $table1 = 'table1';
    my $table2 = 'table2';
    my $table2_alias = 'table2_alias';
    my $table3 = 'table3';
    my $key1 = 'key1';
    my $key2 = 'key2';
    my $key3 = 'key3';
    my $key4 = 'key4';
    my $key5 = 'key5';
    my $key6 = 'key6';
    my $key7 = 'key7';
    my $key8 = 'key8';
    my $key9 = 'key9';
    my $key10 = 'key10';
    
    has table1 => $table1;
    has table2 => $table2;
    has table2_alias => $table2_alias;
    has table3 => $table3;
    has key1 => $key1;
    has key2 => $key2;
    has key3 => $key3;
    has key4 => $key4;
    has key5 => $key5;
    has key6 => $key6;
    has key7 => $key7;
    has key8 => $key8;
    has key9 => $key9;
    has key10 => $key10;

    my $date_typename = 'date';
    my $datetime_typename = 'datetime';

    sub date_typename { lc $date_typename }
    sub datetime_typename { lc $datetime_typename }

    my $date_datatype = -9;
    my $datetime_datatype = 93;

    sub date_datatype { lc $date_datatype }
    sub datetime_datatype { lc $datetime_datatype }

    has exclude_table => sub {
        return qr/^(
          CHECK_CONSTRAINTS
          |COLUMN_DOMAIN_USAGE
          |COLUMN_PRIVILEGES
          |COLUMNS
          |CONSTRAINT_COLUMN_USAGE
          |CONSTRAINT_TABLE_USAGE
          |DOMAIN_CONSTRAINTS
          |DOMAINS
          |KEY_COLUMN_USAGE
          |PARAMETERS
          |REFERENTIAL_CONSTRAINTS
          |ROUTINE_COLUMNS
          |ROUTINES
          |SCHEMATA
          |TABLE_CONSTRAINTS
          |TABLE_PRIVILEGES
          |TABLES
          |VIEW_COLUMN_USAGE
          |VIEW_TABLE_USAGE
          |VIEWS
          |all_columns
          |all_objects
          |all_parameters
          |all_sql_modules
          |all_views
          |allocation_units
          |assemblies
          |assembly_files
          |assembly_modules
          |assembly_references
          |assembly_types
          |asymmetric_keys
          |backup_devices
          |certificates
          |change_tracking_databases
          |change_tracking_tables
          |check_constraints
          |column_type_usages
          |column_xml_schema_collection_usages
          |columns
          |computed_columns
          |configurations
          |conversation_endpoints
          |conversation_groups
          |conversation_priorities
          |credentials
          |crypt_properties
          |cryptographic_providers
          |data_spaces
          |database_audit_specification_details
          |database_audit_specifications
          |database_files
          |database_mirroring
          |database_mirroring_endpoints
          |database_permissions
          |database_principal_aliases
          |database_principals
          |database_recovery_status
          |database_role_members
          |databases
          |default_constraints
          |destination_data_spaces
          |dm_audit_actions
          |dm_audit_class_type_map
          |dm_broker_activated_tasks
          |dm_broker_connections
          |dm_broker_forwarded_messages
          |dm_broker_queue_monitors
          |dm_cdc_errors
          |dm_cdc_log_scan_sessions
          |dm_clr_appdomains
          |dm_clr_loaded_assemblies
          |dm_clr_properties
          |dm_clr_tasks
          |dm_cryptographic_provider_properties
          |dm_database_encryption_keys
          |dm_db_file_space_usage
          |dm_db_index_usage_stats
          |dm_db_mirroring_auto_page_repair
          |dm_db_mirroring_connections
          |dm_db_mirroring_past_actions
          |dm_db_missing_index_details
          |dm_db_missing_index_group_stats
          |dm_db_missing_index_groups
          |dm_db_partition_stats
          |dm_db_persisted_sku_features
          |dm_db_script_level
          |dm_db_session_space_usage
          |dm_db_task_space_usage
          |dm_exec_background_job_queue
          |dm_exec_background_job_queue_stats
          |dm_exec_cached_plans
          |dm_exec_connections
          |dm_exec_procedure_stats
          |dm_exec_query_memory_grants
          |dm_exec_query_optimizer_info
          |dm_exec_query_resource_semaphores
          |dm_exec_query_stats
          |dm_exec_query_transformation_stats
          |dm_exec_requests
          |dm_exec_sessions
          |dm_exec_trigger_stats
          |dm_filestream_file_io_handles
          |dm_filestream_file_io_requests
          |dm_fts_active_catalogs
          |dm_fts_fdhosts
          |dm_fts_index_population
          |dm_fts_memory_buffers
          |dm_fts_memory_pools
          |dm_fts_outstanding_batches
          |dm_fts_population_ranges
          |dm_io_backup_tapes
          |dm_io_cluster_shared_drives
          |dm_io_pending_io_requests
          |dm_os_buffer_descriptors
          |dm_os_child_instances
          |dm_os_cluster_nodes
          |dm_os_dispatcher_pools
          |dm_os_dispatchers
          |dm_os_hosts
          |dm_os_latch_stats
          |dm_os_loaded_modules
          |dm_os_memory_allocations
          |dm_os_memory_brokers
          |dm_os_memory_cache_clock_hands
          |dm_os_memory_cache_counters
          |dm_os_memory_cache_entries
          |dm_os_memory_cache_hash_tables
          |dm_os_memory_clerks
          |dm_os_memory_node_access_stats
          |dm_os_memory_nodes
          |dm_os_memory_objects
          |dm_os_memory_pools
          |dm_os_nodes
          |dm_os_performance_counters
          |dm_os_process_memory
          |dm_os_ring_buffers
          |dm_os_schedulers
          |dm_os_spinlock_stats
          |dm_os_stacks
          |dm_os_sublatches
          |dm_os_sys_info
          |dm_os_sys_memory
          |dm_os_tasks
          |dm_os_threads
          |dm_os_virtual_address_dump
          |dm_os_wait_stats
          |dm_os_waiting_tasks
          |dm_os_worker_local_storage
          |dm_os_workers
          |dm_qn_subscriptions
          |dm_repl_articles
          |dm_repl_schemas
          |dm_repl_tranhash
          |dm_repl_traninfo
          |dm_resource_governor_configuration
          |dm_resource_governor_resource_pools
          |dm_resource_governor_workload_groups
          |dm_server_audit_status
          |dm_tran_active_snapshot_database_transactions
          |dm_tran_active_transactions
          |dm_tran_commit_table
          |dm_tran_current_snapshot
          |dm_tran_current_transaction
          |dm_tran_database_transactions
          |dm_tran_locks
          |dm_tran_session_transactions
          |dm_tran_top_version_generators
          |dm_tran_transactions_snapshot
          |dm_tran_version_store
          |dm_xe_map_values
          |dm_xe_object_columns
          |dm_xe_objects
          |dm_xe_packages
          |dm_xe_session_event_actions
          |dm_xe_session_events
          |dm_xe_session_object_columns
          |dm_xe_session_targets
          |dm_xe_sessions
          |endpoint_webmethods
          |endpoints
          |event_notification_event_types
          |event_notifications
          |events
          |extended_procedures
          |extended_properties
          |filegroups
          |foreign_key_columns
          |foreign_keys
          |fulltext_catalogs
          |fulltext_document_types
          |fulltext_index_catalog_usages
          |fulltext_index_columns
          |fulltext_index_fragments
          |fulltext_indexes
          |fulltext_languages
          |fulltext_stoplists
          |fulltext_stopwords
          |fulltext_system_stopwords
          |function_order_columns
          |http_endpoints
          |identity_columns
          |index_columns
          |indexes
          |internal_tables
          |key_constraints
          |key_encryptions
          |linked_logins
          |login_token
          |master_files
          |master_key_passwords
          |message_type_xml_schema_collection_usages
          |messages
          |module_assembly_usages
          |numbered_procedure_parameters
          |numbered_procedures
          |objects
          |openkeys
          |parameter_type_usages
          |parameter_xml_schema_collection_usages
          |parameters
          |partition_functions
          |partition_parameters
          |partition_range_values
          |partition_schemes
          |partitions
          |plan_guides
          |procedures
          |remote_logins
          |remote_service_bindings
          |resource_governor_configuration
          |resource_governor_resource_pools
          |resource_governor_workload_groups
          |routes
          |schemas
          |securable_classes
          |server_assembly_modules
          |server_audit_specification_details
          |server_audit_specifications
          |server_audits
          |server_event_notifications
          |server_event_session_actions
          |server_event_session_events
          |server_event_session_fields
          |server_event_session_targets
          |server_event_sessions
          |server_events
          |server_file_audits
          |server_permissions
          |server_principal_credentials
          |server_principals
          |server_role_members
          |server_sql_modules
          |server_trigger_events
          |server_triggers
          |servers
          |service_broker_endpoints
          |service_contract_message_usages
          |service_contract_usages
          |service_contracts
          |service_message_types
          |service_queue_usages
          |service_queues
          |services
          |soap_endpoints
          |spatial_index_tessellations
          |spatial_indexes
          |spatial_reference_systems
          |sql_dependencies
          |sql_logins
          |sql_modules
          |stats
          |stats_columns
          |symmetric_keys
          |synonyms
          |syscacheobjects
          |syscharsets
          |syscolumns
          |syscomments
          |sysconfigures
          |sysconstraints
          |syscurconfigs
          |syscursorcolumns
          |syscursorrefs
          |syscursors
          |syscursortables
          |sysdatabases
          |sysdepends
          |sysdevices
          |sysfilegroups
          |sysfiles
          |sysforeignkeys
          |sysfulltextcatalogs
          |sysindexes
          |sysindexkeys
          |syslanguages
          |syslockinfo
          |syslogins
          |sysmembers
          |sysmessages
          |sysobjects
          |sysoledbusers
          |sysopentapes
          |sysperfinfo
          |syspermissions
          |sysprocesses
          |sysprotects
          |sysreferences
          |sysremotelogins
          |sysservers
          |system_columns
          |system_components_surface_area_configuration
          |system_objects
          |system_parameters
          |system_sql_modules
          |system_views
          |systypes
          |sysusers
          |table_types
          |tables
          |tcp_endpoints
          |trace_categories
          |trace_columns
          |trace_event_bindings
          |trace_events
          |trace_subclass_values
          |traces
          |transmission_queue
          |trigger_event_types
          |trigger_events
          |triggers
          |type_assembly_usages
          |types
          |user_token
          |via_endpoints
          |views
          |xml_indexes
          |xml_schema_attributes
          |xml_schema_collections
          |xml_schema_component_placements
          |xml_schema_components
          |xml_schema_elements
          |xml_schema_facets
          |xml_schema_model_groups
          |xml_schema_namespaces
          |xml_schema_types
          |xml_schema_wildcard_namespaces
          |xml_schema_wildcards
        )/x
    };

    my $dsn = "dbi:ODBC:driver={SQL Server};Server={localhost\\SQLEXPRESS};"
      . "Trusted_Connection=No;AutoTranslate=No;Database=dbix_custom;";
    has dsn => $dsn;
    has user  => 'dbix_custom';
    has password => 'dbix_custom';
    
    sub create_table1 { "create table $table1 ($key1 varchar(255), $key2 varchar(255))" }
    sub create_table1_2 {"create table $table1 ($key1 varchar(255), $key2 varchar(255), "
     . "$key3 varchar(255), $key4 varchar(255), $key5 varchar(255))" }
    sub create_table1_type { "create table $table1 ($key1 $date_typename, $key2 $datetime_typename)" }
    sub create_table1_highperformance { "create table $table1 ($key1 varchar(255), $key2 varchar(255), "
      . "$key3 varchar(255), $key4 varchar(255), $key5 varchar(255), $key6 varchar(255), $key7 varchar(255))" }
    sub create_table2 { "create table $table2 ($key1 varchar(255), $key3 varchar(255))" }
    sub create_table2_2 { "create table $table2 ($key1 varchar(255), $key2 varchar(255), $key3 varchar(255))" }
    sub create_table3 { "create table $table3 ($key1 varchar(255), $key2 varchar(255), $key3 varchar(255))" }
    sub create_table_reserved { 'create table "table" ("select" varchar(255), "update" varchar(255))' }
}

require "$FindBin::Bin/common.t";
