{{ config(materialized='view', schema='silver') }}

WITH raw_prices AS (
    SELECT
        -- فك البيانات الأساسية من الـ JSON
        json_data:symbol::string as symbol,
        json_data:price::float as price,
        json_data:volume::int as volume,
        
        -- تحويل الـ Timestamp (من Unix Milliseconds لـ Timestamp)
        TO_TIMESTAMP_NTZ(json_data:timestamp::bigint / 1000) as trade_timestamp,
        
        -- بيانات الـ Metadata (Auditing)
        TO_TIMESTAMP_NTZ(json_data:ingested_at::bigint / 1000) as ingested_at,
        json_data:kafka_partition::int as kafka_partition,
        json_data:kafka_offset::int as kafka_offset,
        file_name
    FROM {{ source('raw_data', 'stocks_raw_data') }}
    -- فلترة عشان نضمن إننا بناخد ملفات الأسعار بس
    WHERE file_name NOT LIKE '%companies_metadata%'
)

SELECT 
    *,
    -- إضافة الـ Asset Class بناءً على الـ Symbol
    CASE 
        WHEN symbol LIKE 'BINANCE:%' THEN 'Crypto'
        ELSE 'Stock'
    END as asset_class
FROM raw_prices