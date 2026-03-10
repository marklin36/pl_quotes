
from datetime import datetime
import pandas as pd
import polars as pl
import pytz
import numpy as np

def get_time_filtered_trades(date_str, start_time_str, end_time_str, columns=None):
    start = datetime.strptime(f"{date_str} {start_time_str}", '%Y%m%d %H:%M:%S')
    end = datetime.strptime(f"{date_str} {end_time_str}", '%Y%m%d %H:%M:%S')
    tz = pytz.timezone('America/New_York')
    start = pd.to_datetime(tz.localize(start)).value
    end = pd.to_datetime(tz.localize(end)).value
    if columns is None:
        columns = ["ticker", "participant_timestamp", "price", "ask_price", "bid_price", "exchange"]
    df = (
        pl.scan_parquet(f"/Volumes/T7/data/trades/signed/{date_str}.parquet")
          .filter(pl.col("participant_timestamp").is_between(start, end))
          .select(columns)
          .collect()
    )
    return df

def add_bsp(df: pl.DataFrame):
    df = df.with_columns(
        (((pl.col('ask_price') + pl.col('bid_price'))/2.0 - pl.col('price'))/((pl.col('ask_price') - pl.col('bid_price'))/2.0))
        .alias('bsp')
    )
    df = df.with_columns(pl.col('bsp').fill_nan(0).fill_null(0))
    df = df.with_columns(
        (pl.col('bsp').clip(lower_bound=-1, upper_bound=1)).alias('bsp')
    )
    return df


def add_block_shuffled_time(df: pl.DataFrame, block=100_000_000) -> pl.DataFrame:

    # --- 1. split timestamp into block + offset ---
    df = df.with_columns((pl.col("participant_timestamp") // block).alias("decisecond_local"))
    df = df.with_columns((pl.col("participant_timestamp") - pl.col("decisecond_local")*block).alias("block_time"))


    # --- 2. build shuffled mapping table ---
    blocks = df.select("decisecond_local").unique().to_series().to_numpy()
    shuffled = blocks.copy()
    np.random.shuffle(shuffled)

    mapping = pl.DataFrame({
        "decisecond_local": blocks,
        "new_block_number": shuffled
    })

    # --- 3. join instead of map(dict) ---
    df = df.join(mapping, on="decisecond_local", how="left")

    # --- 4. rebuild timestamp --- (over write the old timestamp)!
    df = df.with_columns(
        (pl.col("block_time") + pl.col("new_block_number") * block)
        .alias("participant_timestamp")
    )

    return df
