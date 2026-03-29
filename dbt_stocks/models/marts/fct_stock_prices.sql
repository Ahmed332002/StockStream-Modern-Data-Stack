{{ config(
    materialized='incremental',
    unique_key='fact_key',
    on_schema_change='append_new_columns',
    schema='gold'
) }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
    
    {% if is_incremental() %}
        -- الجزء ده بيشتغل بس في الـ Incremental runs
        -- بيجيب الداتا اللي تاريخها أحدث من أحدث تاريخ موجود فعلياً في الجدول في Snowflake
        WHERE trade_timestamp > (SELECT MAX(p_max.trade_timestamp) FROM {{ this }} p_max)
    {% endif %}
),

companies_with_min AS (
    SELECT 
        *,
        MIN(valid_from) OVER (PARTITION BY ticker) as first_ever_valid_from
    FROM {{ ref('dim_companies') }}
),

assets AS (
    SELECT * FROM {{ ref('dim_assets') }}
),

final_selection AS (
    SELECT 
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.symbol', 'p.trade_timestamp']) }} as fact_key,
        c.company_key,
        CAST(p.trade_timestamp as date) as date_key,
        a.asset_key,
        p.price,
        p.volume,
        (p.price * p.volume) as total_value,
        TRIM(UPPER(p.symbol)) as symbol,
        p.trade_timestamp,
        p.ingested_at
    FROM prices p
    LEFT JOIN companies_with_min c 
        ON TRIM(UPPER(p.symbol)) = TRIM(UPPER(c.ticker))
        AND (
            (p.trade_timestamp >= c.valid_from AND (p.trade_timestamp < c.valid_to OR c.valid_to IS NULL))
            OR 
            (c.valid_from = c.first_ever_valid_from AND p.trade_timestamp < c.valid_from)
        )
    LEFT JOIN assets a
        ON p.asset_class = a.asset_class
)

SELECT * FROM final_selection
-- التأكد من عدم تكرار البيانات في الدفعة الجديدة (New Batch)
QUALIFY ROW_NUMBER() OVER (PARTITION BY fact_key ORDER BY ingested_at DESC) = 1