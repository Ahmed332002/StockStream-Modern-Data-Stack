{{ config(materialized='table', schema='gold') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['asset_class']) }} as asset_key,
    asset_class,
    CASE 
        WHEN asset_class = 'Crypto' THEN 'Digital Asset'
        ELSE 'Traditional Equity'
    END as asset_category
FROM (SELECT DISTINCT asset_class FROM {{ ref('stg_stock_prices') }})