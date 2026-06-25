{{ config(materialized='view') }}

{{
  /*
   * stg_erp_machine_states — machine state change records from CDC.
   */
}}

select
    id,
    machine_id,
    state,
    start_time,
    end_time,
    reason
from erp_raw.machine_states
