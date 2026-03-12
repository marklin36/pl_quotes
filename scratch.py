"""
f, r = kernel_g.process_date(
    date=date,
    start_time="10:00:06",
    end_time="15:00:50",
    shuffle='decisecond',
    shift_ns=10_000_000,
    grid_ns=100_000_000,
    n_periods=50,
    upper_count=5,
    filters=[],
    quantile_clip=False,
    add_extra_cols=False,
    data='quotes',
    path='/Volumes/T7/data/quotes/processed',
)
"""

"""
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import kernel_g
import polars as pl

START_DATE = pd.to_datetime('2023-01-01')
END_DATE = pd.to_datetime('2025-11-15')
date_range = pd.bdate_range(START_DATE, END_DATE)
STATISTIC_COL = 'top_5_z_score_abs'


inputs = []
for date in tqdm(date_range):
    #print(date)
    is_there_trade_file = Path(f"/Volumes/T7/data/trades/time_partioned/{date.strftime('%Y%m%d')}.parquet").exists()
    if not is_there_trade_file:
        continue
    f, r = kernel_g.process_date(
        date=date,
        start_time="10:00:06",
        end_time="15:00:50",
        shuffle='decisecond',
        shift_ns=10_000_000,
        grid_ns=100_000_000,
        n_periods=50,
        upper_count=5,
        filters=[],
        quantile_clip=False,
        add_extra_cols=True
    )
    path = "/Volumes/T7/alpha/full_10_15_5s_extra_cols"
    f.write_parquet(f"{path}/{date.strftime('%Y%m%d')}.parquet")
    r.write_parquet(f"{path}/{date.strftime('%Y%m%d')}_R.parquet")
"""
