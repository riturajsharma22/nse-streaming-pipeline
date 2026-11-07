{{ config(
    materialized = 'incremental',
    unique_key = ['symbol', 'candle_ts_utc'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
) }}

select
    symbol,
    candle_ts_utc,
    candle_ts_ist,
    to_date(candle_ts_ist)                              as trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    (high_price + low_price + close_price) / 3          as typical_price,  -- used for VWAP later
    ingested_at,
    loaded_at
from {{ ref('stg_stock_candles') }}

{% if is_incremental() %}
  -- Only reprocess recently loaded rows. The 30-minute lookback catches
  -- candles that were re-sent with revised values; MERGE updates them in place.
  where loaded_at > (select dateadd('minute', -30, max(loaded_at)) from {{ this }})
{% endif %}