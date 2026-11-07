{% macro ohlc_rollup(source_relation, minutes) %}
    -- Rolls 1-minute candles up into N-minute candles.
    -- open = first price in the bucket, close = last price, high/low = extremes.
    select
        symbol,
        trade_date,
        time_slice(candle_ts_ist, {{ minutes }}, 'MINUTE')  as bucket_start_ist,
        min_by(open_price, candle_ts_utc)                   as open_price,
        max(high_price)                                     as high_price,
        min(low_price)                                      as low_price,
        max_by(close_price, candle_ts_utc)                  as close_price,
        sum(volume)                                         as volume,
        count(*)                                            as candles_in_bucket
    from {{ source_relation }}
    group by 1, 2, 3
{% endmacro %}