import polars as pl
import pandas as pd
import argparse

DATA_PATH = "/Volumes/Extreme SSD/data"

def load_data(date):

    trades = pl.scan_parquet(
        f"{DATA_PATH}/trades/clean/{date}.parquet"
    )

    quotes = pl.scan_parquet(
        f"{DATA_PATH}/quotes/clean/{date}.parquet"
    )

    return trades, quotes

def enrich_quotes(quotes):

    q = quotes.sort(["ticker","participant_timestamp"])

    prev = q.select(
        pl.exclude("ticker").shift().name.prefix("prev_")
    )

    q = pl.concat([q, prev], how="horizontal")

    same_price = (
        (pl.col("ask_price")==pl.col("prev_ask_price")) &
        (pl.col("bid_price")==pl.col("prev_bid_price"))
    )

    q = q.with_columns(

        pl.when(
            (pl.col("bid_exchange")!=pl.col("prev_bid_exchange")) &
            (pl.col("ask_exchange")==pl.col("prev_ask_exchange"))
        ).then("bid_ex_change")

        .when(
            (pl.col("bid_exchange")==pl.col("prev_bid_exchange")) &
            (pl.col("ask_exchange")!=pl.col("prev_ask_exchange"))
        ).then("ask_ex_change")

        .when(
            (pl.col("bid_exchange")!=pl.col("prev_bid_exchange")) &
            (pl.col("ask_exchange")!=pl.col("prev_ask_exchange"))
        ).then("bid_ask_ex_change")

        .when(same_price & (pl.col("bid_size") > pl.col("prev_bid_size")))
        .then("bid_size_up")

        .when(same_price & (pl.col("bid_size") < pl.col("prev_bid_size")))
        .then("bid_size_down")

        .otherwise(None)

        .alias("tick_type")
    )

    return q

def build_dataset(trades, quotes):

    trades = trades.sort(["ticker","participant_timestamp"])
    quotes = quotes.sort(["ticker","participant_timestamp"])

    prev_quote = trades.join_asof(
        quotes,
        left_on="participant_timestamp",
        right_on="participant_timestamp",
        by="ticker",
        strategy="backward"
    )

    next_quote = trades.join_asof(
        quotes,
        left_on="participant_timestamp",
        right_on="participant_timestamp",
        by="ticker",
        strategy="forward",
        suffix="_next"
    )

    trades = prev_quote.join(
        next_quote.select(
            "ticker",
            "participant_timestamp",
            pl.col("participant_timestamp_next")
        ),
        on=["ticker","participant_timestamp"]
    )

    trades = trades.with_columns(

        (
            pl.col("participant_timestamp")
            - pl.col("participant_timestamp_right")
        ).alias("t_from_prev_quote"),

        (
            pl.col("participant_timestamp_next")
            - pl.col("participant_timestamp")
        ).alias("t_to_next_quote")
    )

    return trades

def trade_signing(frame):

    mid = (pl.col("ask_price")+pl.col("bid_price"))/2
    spread = (pl.col("ask_price")-pl.col("bid_price"))/2

    return frame.with_columns(
        ((pl.col("price")-mid)/spread).alias("bsp")
    )


def process_day(date, tickers):

    trades, quotes = load_data(date)

    trades = trades.filter(pl.col("ticker").is_in(tickers))
    quotes = quotes.filter(pl.col("ticker").is_in(tickers))

    quotes = enrich_quotes(quotes)

    frame = build_dataset(trades, quotes)

    frame = trade_signing(frame)

    return frame.collect(streaming=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Onboard Trades")
    parser.add_argument("--start_date", type=str)
    parser.add_argument("--end_date", type=str)
    parser.add_argument("--quote_data_dir", type=str)
    parser.add_argument("--trade_data_dir", type=str)
    parser.add_argument("--universe_path", type=str)

    args = parser.parse_args()

    date_range = pd.bdate_range(args.start_date, args.end_date)

    univ = pl.read_parquet(f"{DATA_PATH}/universe.parquet")

    for date in date_range:
        tickers = (
            univ
            .filter(pl.col("date") == date)
            .select("ticker")
            .unique()
            .to_series()
            .implode()
        )

        df = process_day(date.strftime("%Y%m%d"), tickers)

        df.write_parquet(
            f"{DATA_PATH}/trades_and_quotes/{date.strftime('%Y%m%d')}.parquet"
        )