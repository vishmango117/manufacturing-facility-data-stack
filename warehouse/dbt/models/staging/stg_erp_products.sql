{{ config(materialized='view') }}

{{
  /*
   * stg_erp_products — clean product table from CDC landing.
   */
}}

select
    id,
    name,
    sku,
    family,
    uom,
    is_active
from erp_raw.products
