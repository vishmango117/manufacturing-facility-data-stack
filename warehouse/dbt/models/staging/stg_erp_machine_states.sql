{{ config(materialized='view') }}

{#- stg_erp_machine_states — machine state intervals from ERP CDC. Returns empty when no CDC data yet. -#}

{% if adapter.get_relation(database=target.database, schema='erp_raw', identifier='machine_states') %}
select
    id,
    machine_id,
    state,
    start_time,
    end_time,
    reason
from erp_raw.machine_states
{% else %}
select
    null::integer    as id,
    null::text       as machine_id,
    null::text       as state,
    null::timestamp  as start_time,
    null::timestamp  as end_time,
    null::text       as reason
where false
{% endif %}
