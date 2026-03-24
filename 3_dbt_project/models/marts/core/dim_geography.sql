{{ config(
    materialized='table',
    indexes=[
      {'columns': ['geography_key'], 'unique': True}
    ]
) }}

with staging as (
    select * from {{ ref('stg_nike_global') }}
),

unique_countries as (
    select distinct
        country_code,
        currency
    from staging
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['country_code', 'currency']) }} as geography_key,
        country_code,
        currency
    from unique_countries
)

select * from final
