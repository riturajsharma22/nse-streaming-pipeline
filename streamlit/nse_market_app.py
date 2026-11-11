"""NSE Market Pipeline: Streamlit in Snowflake app.

Reads only the production dbt marts (PROD_MARTS) plus the raw landing table
for pipeline health. Create it in Snowsight as a Streamlit app in
NSE_STREAM.PUBLIC with query warehouse NSE_WH, then paste this file.
"""
import altair as alt
import streamlit as st

st.set_page_config(page_title="NSE Market Pipeline", layout="wide")

# Works on both the warehouse runtime and the newer container runtime
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    session = st.connection("snowflake").session()

session.use_warehouse("NSE_WH")

st.title("📈 NSE Market Pipeline")
st.caption("1-minute NSE candles · EventBridge → Lambda → S3 → Snowpipe → dbt → Snowflake")


@st.cache_data(ttl=300)  # 5-minute cache: clicking around doesn't keep waking the warehouse
def run(sql, params=None):
    return session.sql(sql, params=params).to_pandas()


# ---------- Pipeline health ----------
h = run("""
    select
        datediff('minute', max(loaded_at), current_timestamp())           as minutes_since_last_load,
        count(*)                                                          as raw_rows,
        (select count(*) from NSE_STREAM.PROD_MARTS.FCT_STOCK_CANDLES_1M) as clean_candles
    from NSE_STREAM.RAW.STOCK_CANDLES_RAW
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Minutes since last load", int(h["MINUTES_SINCE_LAST_LOAD"]))
c2.metric("Raw rows landed", f"{int(h['RAW_ROWS']):,}")
c3.metric("Clean candles", f"{int(h['CLEAN_CANDLES']):,}")
c4.metric("Duplicates removed", f"{100 * (1 - h['CLEAN_CANDLES'] / h['RAW_ROWS']):.1f}%")

st.divider()

# ---------- Filters ----------
symbols = run("""
    select distinct symbol
    from NSE_STREAM.PROD_MARTS.MART_INTRADAY_METRICS
    order by 1
""")["SYMBOL"].tolist()

dates = run("""
    select distinct trade_date
    from NSE_STREAM.PROD_MARTS.MART_INTRADAY_METRICS
    order by 1 desc
""")["TRADE_DATE"].tolist()

f1, f2 = st.columns(2)
symbol = f1.selectbox("Stock", symbols)
trade_date = str(f2.selectbox("Trading day", dates))

# ---------- Price vs VWAP ----------
# Bind parameters (?) instead of string formatting keep the SQL safe from injection
st.subheader(f"{symbol}: price vs VWAP")
intraday = run("""
    select candle_ts_ist,
           close_price::float as close,
           vwap::float        as vwap,
           sma_20m::float     as sma_20m
    from NSE_STREAM.PROD_MARTS.MART_INTRADAY_METRICS
    where symbol = ? and trade_date = ?
    order by candle_ts_ist
""", params=[symbol, trade_date])

chart_df = intraday.melt("CANDLE_TS_IST", var_name="series", value_name="price")
price_chart = (
    alt.Chart(chart_df)
    .mark_line()
    .encode(
        x=alt.X("CANDLE_TS_IST:T", title="Time (IST)"),
        y=alt.Y("price:Q", scale=alt.Scale(zero=False), title="Price (₹)"),  # fit the axis to the price range
        color=alt.Color("series:N", title=None),
        tooltip=["CANDLE_TS_IST:T", "series:N", alt.Tooltip("price:Q", format=".2f")],
    )
    .properties(height=380)
)
st.altair_chart(price_chart, width="stretch")

# ---------- Day change % and 15-minute candles ----------
left, right = st.columns(2)

with left:
    st.subheader("Day change % by stock")
    daily = run("""
        select symbol, day_change_pct::float as day_change_pct
        from NSE_STREAM.PROD_MARTS.MART_DAILY_SUMMARY
        where trade_date = ?
        order by day_change_pct desc
    """, params=[trade_date])
    st.bar_chart(daily.set_index("SYMBOL")["DAY_CHANGE_PCT"])

with right:
    st.subheader("15-minute candles")
    candles = run("""
        select bucket_start_ist, open_price, high_price, low_price, close_price, volume
        from NSE_STREAM.PROD_MARTS.MART_CANDLES_15M
        where symbol = ? and trade_date = ?
        order by bucket_start_ist
    """, params=[symbol, trade_date])
    st.dataframe(candles, width="stretch", hide_index=True)

# ---------- Daily summary ----------
st.subheader("Daily summary")
summary = run("""
    select trade_date, symbol, day_open, day_high, day_low, day_close,
           day_volume, round(day_vwap, 2) as day_vwap, day_change_pct, candles_captured
    from NSE_STREAM.PROD_MARTS.MART_DAILY_SUMMARY
    order by trade_date desc, symbol
""")
st.dataframe(summary, width="stretch", hide_index=True)
