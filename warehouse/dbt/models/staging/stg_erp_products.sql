{{ config(materialized='view') }}

select
    product_id,
    name,
    sku,
    family,
    uom
from erp_raw.products
