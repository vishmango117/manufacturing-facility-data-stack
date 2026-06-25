{{ config(materialized='table') }}

{#- dim_product — product/SKU dimension from ERP. -#}

select
    row_number() over ()                                as product_key,
    product_id,
    name                                                as product_name,
    sku,
    family,
    uom
from erp_raw.products
