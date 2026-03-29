{{ config(materialized='view', schema='silver') }}

with raw_source as (
    select 
        json_data, 
        ingested_at 
    from {{ source('raw_data', 'stocks_raw_data') }}
    where file_name like '%companies_metadata%'
),

flattened_data as (
    select
        -- بنستخدم value بدل json_data[0] عشان نجيب كل العناصر
        value['ticker']::string as ticker,
        value['name']::string as company_name,
        value['finnhubIndustry']::string as industry,
        value['exchange']::string as exchange,
        value['country']::string as country,
        value['currency']::string as currency,
        value['marketCapitalization']::float as market_cap,
        value['shareOutstanding']::float as shares_outstanding,
        value['ipo']::date as ipo_date,
        value['phone']::string as phone,
        value['weburl']::string as web_url,
        value['logo']::string as logo_url,
        -- الـ ingested_at بناخدها من العمود اللي بره الـ JSON
        ingested_at::timestamp as ingested_at
    from raw_source,
    lateral flatten(input => json_data) -- بيفك الـ Array لصفوف
)

select * from flattened_data