-- models/marts/core/dim_product.sql

with staging as (
    select * from {{ ref('stg_nike_global') }}
),

unique_products as (
    -- One row per SKU with the most frequent or descriptive name
    select 
        sku,
        max(product_name) as product_name, -- Taking max to pick one Representative name
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
