with candles as (

    select * from {{ ref('fct_stock_candles_1m') }}

),

metrics as (

    select
        symbol,
        trade_date,
        candle_ts_ist,
        close_price,
        volume,

        -- Running VWAP for the day: total (price x volume) / total volume so far
        sum(typical_price * volume) over (
            partition by symbol, trade_date
            order by candle_ts_utc
            rows between unbounded preceding and current row
        ) / nullif(sum(volume) over (
            partition by symbol, trade_date
            order by candle_ts_utc
            rows between unbounded preceding and current row
        ), 0)                                                   as vwap,

        -- 20-minute simple moving average of the close
        avg(close_price) over (
            partition by symbol, trade_date
            order by candle_ts_utc
            rows between 19 preceding and current row
        )                                                       as sma_20m,

        -- Minute-over-minute return
        close_price / nullif(lag(close_price) over (
            partition by symbol, trade_date
            order by candle_ts_utc
        ), 0) - 1                                               as return_1m

    from candles

)

select
    *,
    close_price > vwap as is_above_vwap
from metrics