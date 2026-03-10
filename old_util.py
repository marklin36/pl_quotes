import pandas as pd
from datetime import datetime
import pytz

def add_dt(df):
    df['dt'] = pd.to_datetime(df['participant_timestamp'], unit='ns', origin='unix', utc=True).copy()
    df['dt'] = df['dt'].dt.tz_convert('America/New_York')
    return df


# suprisingly slow
def filter_time(df, start_time, end_time):

    if 'dt' not in df.columns:
        df = add_dt(df)

    dates = df['dt'].dt.date.unique()
    assert len(dates) == 1
    date = dates[0]
    date_str = date.strftime('%Y%m%d')

    start = datetime.strptime(f"{date_str} {start_time}", '%Y%m%d %H:%M:%S')
    end = datetime.strptime(f"{date_str} {end_time}", '%Y%m%d %H:%M:%S')

    tz = pytz.timezone('America/New_York')
    start = tz.localize(start)
    end = tz.localize(end)

    df = df[(df['dt'] >= start)&(df['dt'] <= end)].copy()
    return df

def add_alphabetical_ticker_rank(df):
    frame = df.drop_duplicates('ticker')
    frame['ticker_rank'] = frame['ticker'].rank(ascending=True)
    new = df.merge(frame[['ticker', 'ticker_rank']], on='ticker')
    return new


def fast_filter_time(df, start_time, end_time):
    if 'dt' not in df.columns:
        df = add_dt(df)

    dates = df['dt'].dt.date.unique()
    assert len(dates) == 1
    date = dates[0]
    date_str = date.strftime('%Y%m%d')

    start = datetime.strptime(f"{date_str} {start_time}", '%Y%m%d %H:%M:%S')
    end = datetime.strptime(f"{date_str} {end_time}", '%Y%m%d %H:%M:%S')

    tz = pytz.timezone('America/New_York')
    start = tz.localize(start)
    end = tz.localize(end)

    start = pd.Timestamp(start).value
    end = pd.Timestamp(end).value

    df = df[(df['participant_timestamp'] >= start)&(df['participant_timestamp'] <= end)].copy()
    return df


# TODO: reimplement in a simpler way
def get_bar(date_str, time_str):
    dt = datetime.strptime(f"{date_str} {time_str}", '%Y%m%d %H:%M:%S')
    hour = dt.hour - 9
    minute = dt.minute
    if hour == 0:
        minute = minute - 30
    minute = minute // 5
    print(hour, minute)
    print(hour * 11 + minute)


def alt_get_bar(date_str, time_str):
    dt = datetime.strptime(f"{date_str} {time_str}", '%Y%m%d %H:%M:%S')
    minutes = dt.hour*60 + dt.minute
    minutes = minutes - 570
    bar = minutes // 5
    return bar


def get_signed_trades(date_str, start_time_str, end_time_str, disk="T7"):
    start_bar = alt_get_bar(date_str, start_time_str)
    end_bar = alt_get_bar(date_str, end_time_str)
    df = pd.read_parquet(
        path=f"/Volumes/{disk}/data/trades/time_partioned/{date_str}.parquet",
        filters=[("five_min_bar", ">=", start_bar), ("five_min_bar", "<=", end_bar)]
    )
    return df



