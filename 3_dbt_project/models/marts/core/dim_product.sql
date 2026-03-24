
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['product_key'], 'unique': True}
    ]
) }}

with staging as (
    select * from {{ ref('stg_nike_global') }}
),

unique_products as (

    select 
        sku,
        max(product_name) as product_name, 
        max(category) as category,
        max(subcategory) as subcategory,
        max(gender) as gender
    from staging
    group by 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sku']) }} as product_key,
        sku,
        product_name,
        category,
        subcategory,
        gender
    from unique_products
)

select * from final
