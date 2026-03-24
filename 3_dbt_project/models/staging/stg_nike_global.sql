-- models/staging/stg_nike_global.sql

with source as (
    select * from {{ source('raw_nike', 'global_nike') }}
),

renamed as (
    select
        -- Create a unique ID for each record based on its primary key
        {{ dbt_utils.generate_surrogate_key(['snapshot_date', 'country_code', 'sku']) }} as nike_id,
        cast(snapshot_date as date) as snapshot_date,
        country_code,
        sku,
        product_name,
        category,
        subcategory,
        trim(gender_segment) as gender_segment,
        currency,
        cast(nullif(price_local, 'None') as numeric) as price_local,
        cast(nullif(sale_price_local, 'None') as numeric) as sale_price_local,
        record_source
    from source
    where cast(nullif(price_local, 'None') as numeric) < 100000 
       or price_local is null
),

cleaned as (
    select
        *,
        -- Price correction logic: Use the minimum of price and sale_price (to handle the 210k errors detected)
        least(price_local, coalesce(sale_price_local, price_local)) as effective_price_local,
        
        -- Gender normalization logic
        case 
            when gender_segment = 'MEN' then 'MEN'
            when gender_segment = 'WOMEN' then 'WOMEN'
            when gender_segment in ('MEN|WOMEN', 'WOMEN|MEN') then 'UNISEX'
            when gender_segment like '%BOYS%' or gender_segment like '%GIRLS%' then 'KIDS'
            else 'OTHER'
        end as gender_normalized
    from renamed
),

deduplicated as (
    -- Remove the 13k duplicates by keeping the most recent or highest quality record
    select * from (
        select 
            *,
            row_number() over (
                partition by snapshot_date, country_code, sku 
                order by record_source desc
            ) as rn
        from cleaned
    ) as sub
    where rn = 1
),

exchange_rates as (
    select * from {{ ref('currency_exchange_rates') }}
),

with_usd as (
    select
        d.nike_id,
        d.snapshot_date,
        d.country_code,
        d.sku,
        d.product_name,
        d.category,
        d.subcategory,
        d.gender_normalized as gender,
        d.currency,
        d.price_local,
        d.sale_price_local,
        d.effective_price_local,
        d.record_source,
        er.to_usd_rate,
        case 
            when er.to_usd_rate is not null then d.price_local * er.to_usd_rate
            when trim(upper(d.currency)) = 'USD' then d.price_local
            else null 
        end as price_usd,
        case 
            when er.to_usd_rate is not null then d.sale_price_local * er.to_usd_rate
            when trim(upper(d.currency)) = 'USD' then d.sale_price_local
            else null 
        end as sale_price_usd,
        case 
            when er.to_usd_rate is not null then d.effective_price_local * er.to_usd_rate
            when trim(upper(d.currency)) = 'USD' then d.effective_price_local
            else null 
        end as effective_price_usd
    from deduplicated d
    left join exchange_rates er on trim(upper(d.currency)) = trim(upper(er.currency_code))
)

select 
    nike_id,
    snapshot_date,
    country_code,
    sku,
    product_name,
    category,
    subcategory,
    gender,
    currency,
    price_local,
    sale_price_local,
    effective_price_local,
    price_usd,
    sale_price_usd,
    effective_price_usd,
    record_source
from with_usd
