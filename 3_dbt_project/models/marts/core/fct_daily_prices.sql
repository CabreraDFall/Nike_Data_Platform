-- models/marts/core/fct_daily_prices.sql

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
        -- Metrics
        (price_local - effective_price_local) as discount_amount,
        case 
            when price_local > 0 then (price_local - effective_price_local) / price_local 
            else 0 
        end as discount_percentage_local
    from staging
)

select * from final
