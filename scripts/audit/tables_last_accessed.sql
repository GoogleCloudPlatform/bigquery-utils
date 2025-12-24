/*
 * BigQuery Table Access and Usage Analysis Script
 *
 * This script analyzes BigQuery job history to determine when tables were last accessed
 * and provides insights into Data Manipulation Language (DML) and Data Definition Language (DDL)
 * operations performed on those tables.
 *
 * It helps identify:
 * - The last time a table was referenced in a query (last accessed time).
 * - The frequency of SELECT, INSERT, UPDATE, DELETE, TRUNCATE, and other DDL/DML operations.
 *
 * This information is valuable for:
 * - Identifying unused tables for potential deletion to optimize storage costs.
 * - Understanding table usage patterns for performance tuning and resource allocation.
 */


-- Set 'first_load' to TRUE for the initial run to scan the last 180 days of job history.
-- Set 'first_load' to FALSE for subsequent runs to scan the last 2 days.
declare first_load bool default false;
declare days_to_scan int64 default if(first_load, 180, 2);

-- Update the admin project beloe
declare admin_project string default '<default-admin-project>';

-- the BQ greatest function does not exclude nulls from comparison
-- below function mimics the greatest function, but excludes nulls.
create temp function fn_greatest(arr any type) as (
	(select max(x) x_max
	from unnest(arr) x
	where x is not null)
);

-- extract relevant details from information_schema.jobs
create or replace temp table `temp_jobs_referenced_tables` as
select creation_time, statement_type, referenced_tables, destination_table
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION 
where regexp_contains(reservation_id, admin_project)
and date(creation_time) >= current_date - days_to_scan; 

-- create table to store the table access details 
create table if not exists `optimization_workshop.tables_access_summary`
(
	  report_date date 
	, table_catalog string 
	, table_schema string 
	, table_name string 
	, last_accessed_time timestamp 
	, select_count int64 
	, update_count int64 
	, delete_count int64
	, insert_count int64
	, merge_count int64
	, truncate_table_count int64
	, create_table_count int64
	, create_table_as_select_count int64
	, create_view_count int64
	, load_data_count int64
	, materialized_count int64
	, drop_table_count int64 
	, create_snapshot_table_count int64
)
partition by report_date 
cluster by table_catalog, table_schema, table_name;

-- delete existing data for the scanned date range 
delete from `optimization_workshop.tables_access_summary`
where report_date >= current_date - days_to_scan;

-- populate the table
insert into `optimization_workshop.tables_access_summary`
with ref_tables as (
	select 
	  date(creation_time) report_date 
	, ref_tables.project_id
	, ref_tables.dataset_id
	, ref_tables.table_id
	, max(creation_time) last_accessed_time
	-- tables under the referenced_tables will only be part of select/from
	, count(1) as select_count
	, 0 update_count
	, 0 delete_count
	, 0 insert_count
	, 0 merge_count
	, 0 truncate_table_count
	, 0 create_table_count
	, 0 create_table_as_select_count
	, 0 create_view_count
	, 0 load_data_count
	, 0 materialized_count
	, 0 drop_table_count
	, 0 create_snapshot_table_count 
	from `optimization_workshop.tables_access_summary`
	, unnest(referenced_tables) as ref_tables 
	group by all 
), dest_tables as (
	select 
	  date(creation_time) report_date 
	, destination_table.project_id
	, destination_table.dataset_id , destination_table.table_id , max(creation_time) last_accessed_time
	, 0 as select_count
	, sum(case when statement_type = 'UPDATE' then 1 else 0 end) update_count
	, sum(case when statement_type = 'DELETE' then 1 else 0 end) delete_count
	, sum(case when statement_type = 'INSERT' then 1 else 0 end) insert_count
	, sum(case when statement_type = 'MERGE' then 1 else 0 end) merge_count
	, sum(case when statement_type = 'TRUNCATE_TABLE' then 1 else 0 end) truncate_table_count
	, sum(case when statement_type = 'CREATE_TABLE' then 1 else 0 end) create_table_count
	, sum(case when statement_type = 'CREATE_TABLE_AS_SELECT' then 1 else 0 end) create_table_as_select_count
	, sum(case when statement_type = 'CREATE_VIEW' then 1 else 0 end) create_view_count
	, sum(case when statement_type = 'LOAD_DATA' then 1 else 0 end) load_data_count
	, sum(case when statement_type = 'CREATE_MATERIALIZED_VIEW' then 1 else 0 end) materialized_count
	, sum(case when statement_type = 'DROP_TABLE' then 1 else 0 end) drop_table_count
	, sum(case when statement_type = 'CREATE_SNAPSHOT_TABLE' then 1 else 0 end) create_snapshot_table_count 
	from `optimization_workshop.tables_access_summary`
	-- every select statement will have a temp table as destination, skip it
	where statement_type not in ('SELECT')
  group by all
)
select 
  coalesce(r.report_date, d.report_date) report_date
, coalesce(r.project_id, d.project_id) table_catalog
, coalesce(r.dataset_id, d.dataset_id) table_schema
, coalesce(r.table_id , d.table_id) table_name
, fn_greatest([r.last_accessed_time, d.last_accessed_time]) as last_accessed_time
, r.select_count select_count
, d.update_count
, d.delete_count
, d.insert_count
, d.merge_count
, d.truncate_table_count
, d.create_table_count
, d.create_table_as_select_count
, d.create_view_count
, d.load_data_count
, d.materialized_count
, d.drop_table_count
, d.create_snapshot_table_count 	
from ref_tables r full outer join dest_tables d
on r.report_date = d.report_date
and r.project_id = d.project_id 
and r.dataset_id = d.dataset_id 
and r.table_id = d.table_id;


-- DML and DDL counts on a table for last 30 days
select table_catalog
, table_schema 
, table_name 
, sum(select_count) select_count
, sum(update_count) update_count
, sum(delete_count) delete_count
, sum(insert_count) insert_count
, sum(merge_count) merge_count
, sum(truncate_table_count) truncate_table_count
, sum(create_table_count) create_table_count
, sum(create_table_as_select_count) create_table_as_select_count
, sum(create_view_count) create_view_count
, sum(load_data_count) load_data_count
, sum(materialized_count) materialized_count
, sum(drop_table_count) drop_table_count
, sum(create_snapshot_table_count) create_snapshot_table_count
from `optimization_workshop.tables_access_summary` 
where report_date > current_date - 30
group by all;


-- table last accessed
select table_catalog
, table_name
, max(last_accessed_time) last_accessed_time
from `optimization_workshop.tables_access_summary` 
group by all;

