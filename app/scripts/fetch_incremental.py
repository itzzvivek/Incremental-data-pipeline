import os
import requests
import pyspark as pd
from datetime import datetime
from .metadata import load_metadata


API_KEY = os.getenv("COINAPI_KEY")
BASE_UR = os.getenv("COINAPI_OHLC_ENDPOINT", "https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history")

def fetch_incremental():
    start_time = load_metadata()
    #CoinAPI expect ISO8601
    params = {
        "period_id": "1HRS",
        "time_start": start_time.isoformat(),
        "limit": 10000,
    }
    headers = {"X-CoinAPI-Key": API_KEY}
    r = requests.get(BASE_UR, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not data:
        return pd.DataFrame()
    

    df = pd.DataFrame(data)
    # normalize timestamps
    df['time_period_start'] = pd.to_datetime(df['time_period_start'])
    df['time_period_end'] = pd.to_datetime(df['time_period_end'])

    #filter strictly greater than last loaded
    df = df[df["time_period_start"] > start_time]
    return df
