import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_NAME"]
SYMBOLS = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS"]
LOOKBACK_CANDLES = 5  # re-send recent candles; dbt will deduplicate


def fetch_candles(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1m&range=1d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    result = data["chart"]["result"][0]
    timestamps = result.get("timestamp") or []
    quote = result["indicators"]["quote"][0]
    meta = result["meta"]

    rows = []
    # Skip the very last candle: the current minute is still forming
    start = max(0, len(timestamps) - 1 - LOOKBACK_CANDLES)
    for i in range(start, len(timestamps) - 1):
        if quote["close"][i] is None:
            continue  # Yahoo gap: no data for this minute yet
        rows.append({
            "symbol": symbol,
            "candle_ts": timestamps[i],
            "open": quote["open"][i],
            "high": quote["high"][i],
            "low": quote["low"][i],
            "close": quote["close"][i],
            "volume": quote["volume"][i],
            "currency": meta.get("currency"),
            "exchange": meta.get("exchangeName"),
        })
    return rows


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    records, errors = [], []

    for symbol in SYMBOLS:
        try:
            records.extend(fetch_candles(symbol))
        except Exception as e:  # one bad symbol shouldn't kill the batch
            errors.append({"symbol": symbol, "error": str(e)})

    for r in records:
        r["ingested_at"] = now.isoformat()
        r["batch_id"] = context.aws_request_id

    key = None
    if records:
        key = (
            f"raw/stocks/year={now:%Y}/month={now:%m}/day={now:%d}/hour={now:%H}/"
            f"batch_{now:%Y%m%dT%H%M%S}_{context.aws_request_id[:8]}.json"
        )
        body = "\n".join(json.dumps(r) for r in records)  # JSON Lines, like Firehose
        s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"),
                      ContentType="application/json")

    summary = {"records": len(records), "errors": errors, "s3_key": key}
    print(json.dumps(summary))
    return summary
