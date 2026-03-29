{{ config(
    materialized='table',
    schema='gold'
    
) }}

-- 1. Get real data from the snapshot (SCD Type 2)
WITH snapshot_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ticker', 'dbt_valid_from']) }} as company_key,
        ticker,
        company_name,
        industry,
        exchange,
        country,
        currency,
        market_cap,
        shares_outstanding,
        ipo_date,
        logo_url,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END as is_current
    FROM {{ ref('scd_companies') }}
),

-- 2. Identify symbols from prices that are missing in the company metadata
missing_symbols AS (
    SELECT DISTINCT symbol 
    FROM {{ ref('stg_stock_prices') }}
    WHERE TRIM(UPPER(symbol)) NOT IN (SELECT DISTINCT TRIM(UPPER(ticker)) FROM snapshot_data)
),

-- 3. Create dynamic dummy rows for these missing symbols (like Crypto)
dummy_rows AS (
    SELECT
        -- The key is the symbol itself to match the Left Join in the Fact table
        {{ dbt_utils.generate_surrogate_key(['symbol', "CAST('1900-01-01' as TIMESTAMP)"]) }} as company_key, 
        symbol as ticker,
        'Non-Stock Asset (Crypto/Other)' as company_name,
        'Digital Assets' as industry,
        'Binance/External' as exchange,
        'Global' as country,
        'USD' as currency,
        0 as market_cap,
        0 as shares_outstanding,
        CAST('1900-01-01' as DATE) as ipo_date,
        NULL as logo_url,
        CAST('1900-01-01' as TIMESTAMP) as valid_from,
        NULL as valid_to,
        TRUE as is_current
    FROM missing_symbols
)

-- 4. Combine real companies with the newly created asset records
SELECT * FROM snapshot_data
UNION ALL
SELECT * FROM dummy_rows