{% snapshot scd_companies %}

{{
    config(
      target_schema='silver',
      unique_key='ticker',
      strategy='check',
      check_cols=['market_cap', 'industry', 'exchange', 'shares_outstanding'],
    )
}}

select * from {{ ref('stg_companies') }}

{% endsnapshot %}