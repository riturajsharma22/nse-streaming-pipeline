select
    symbol,
    trade_date,
    min_by(open_price, candle_ts_utc)                           as day_open,
    max(high_price)                                             as day_high,
    min(low_price)                                              as day_low,
    max_by(close_price, candle_ts_utc)                          as day_close,
    sum(volume)                                                 as day_volume,
    sum(typical_price * volume) / nullif(sum(volume), 0)        as day_vwap,
    round((max_by(close_price, candle_ts_utc)
           / nullif(min_by(open_price, candle_ts_utc), 0) - 1) * 100, 2)
                                                                as day_change_pct,
    count(*)                                                    as candles_captured,
    min(candle_ts_ist)                                          as first_candle_ist,
    max(candle_ts_ist)                                          as last_candle_ist
from {{ ref('fct_stock_candles_1m') }}
group by 1, 2