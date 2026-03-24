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
)

select 
    nike_id,
    snapshot_date,
    country_code,
    sku,
    product_name,
    category,
    subcategory,
    gender_normalized as gender,
    currency,
    price_local,
    sale_price_local,
    effective_price_local,
    record_source
from deduplicated
