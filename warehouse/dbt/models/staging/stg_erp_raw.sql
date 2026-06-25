{{ config(materialized='view') }}

{#- stg_erp_raw — inventory of all CDC-landed erp_raw tables for exploratory use. -#}

select 'machines'        as table_name, count(*) as row_count from erp_raw.machines
union all
select 'products'        as table_name, count(*) as row_count from erp_raw.products
union all
select 'shifts'          as table_name, count(*) as row_count from erp_raw.shifts
union all
select 'work_orders'     as table_name, count(*) as row_count from erp_raw.work_orders
