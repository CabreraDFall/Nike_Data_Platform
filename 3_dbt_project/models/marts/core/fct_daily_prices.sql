
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_day'], 'type': 'btree'},
      {'columns': ['product_key'], 'type': 'btree'},
      {'columns': ['geography_key'], 'type': 'btree'}
    ],
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["product_key", "geography_key"]
) }}

with staging as (
    select * from {{ ref('stg_nike_global') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'country_code', 'sku']) }} as fact_key,
        {{ dbt_utils.generate_surrogate_key(['sku']) }} as product_key,
        {{ dbt_utils.generate_surrogate_key(['country_code', 'currency']) }} as geography_key,
        snapshot_date as date_day,
        price_local,
        sale_price_local,
        effective_price_local,
        price_usd,
        sale_price_usd,
        effective_price_usd,
        
        (price_local - effective_price_local) as discount_amount,
        (price_usd - effective_price_usd) as discount_amount_usd,
        case 
            when price_local > 0 then (price_local - effective_price_local) / price_local 
            else 0 
        end as discount_percentage_local
    from staging
)

select * from final
