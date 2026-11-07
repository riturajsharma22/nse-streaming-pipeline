{{ config(severity = 'warn') }}

-- A valid candle: low <= open/close <= high, and volume can't be negative.
-- 'warn' means a bad tick from the source is flagged without blocking the pipeline.
select *
from {{ ref('fct_stock_candles_1m') }}
where low_price > high_price
   or open_price  not between low_price and high_price
   or close_price not between low_price and high_price
   or volume < 0