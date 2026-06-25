{{ config(materialized='view') }}

{{
  /*
   * stg_erp_raw — dedupe CDC records from erp_raw.* to latest per primary key.
   *
   * Debezium CDC produces one row per change event with __OP, __TS_MS columns.
   * We keep the latest row per PK for each table.
   */
}}

{% for table in ['products', 'machines', 'work_orders', 'production_runs', 'machine_states', 'shifts'] %}
select * from erp_raw.{{ table }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
