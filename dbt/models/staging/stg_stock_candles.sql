with source as (

    select * from {{ source('raw', 'stock_candles_raw') }}

),

parsed as (

    select
        record:symbol::string                                   as symbol,
        to_timestamp_ntz(record:candle_ts::number)              as candle_ts_utc,
        convert_timezone('UTC', 'Asia/Kolkata',
            to_timestamp_ntz(record:candle_ts::number))         as candle_ts_ist,
        record:open::number(18, 4)                              as open_price,
        record:high::number(18, 4)                              as high_price,
        record:low::number(18, 4)                               as low_price,
        record:close::number(18, 4)                             as close_price,
        record:volume::number                                   as volume,
        record:currency::string                                 as currency,
        record:exchange::string                                 as exchange,
        record:batch_id::string                                 as batch_id,
        try_to_timestamp_tz(record:ingested_at::string)         as ingested_at,
        source_file,
        loaded_at
    from source

),

deduplicated as (

    -- The producer re-sends recent candles every minute (at-least-once delivery).
    -- Keep one row per symbol + minute: the most recently ingested version,
    -- since Yahoo can revise a candle's volume shortly after the minute closes.
    select *
    from parsed
    qualify row_number() over (
        partition by symbol, candle_ts_utc
        order by ingested_at desc, loaded_at desc
    ) = 1

)

select * from deduplicated