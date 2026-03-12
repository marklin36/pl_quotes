import polars as pl
import pandas as pd
import argparse
from tqdm import tqdm

DATA_PATH = "/Volumes/Extreme SSD/data"

def load_data(date, trades_path, quotes_path):

    trades = pl.scan_parquet(
        f"{trades_path}/{date}.parquet"
    )

    quotes = pl.scan_parquet(
        f"{quotes_path}/{date}.parquet"
    )


    return trades, quotes


def enrich_quotes(quotes):

    q = quotes.with_columns(

        pl.col("bid_exchange").shift().over("ticker").alias("prev_bid_exchange"),
        pl.col("ask_exchange").shift().over("ticker").alias("prev_ask_exchange"),

        pl.col("bid_price").shift().over("ticker").alias("prev_bid_price"),
        pl.col("ask_price").shift().over("ticker").alias("prev_ask_price"),

        pl.col("bid_size").shift().over("ticker").alias("prev_bid_size"),
        pl.col("ask_size").shift().over("ticker").alias("prev_ask_size"),
    )

    same_price = (
        (pl.col("ask_price") == pl.col("prev_ask_price")) &
        (pl.col("bid_price") == pl.col("prev_bid_price"))
    )

    tick_type_expr = (

        pl.when(
            (pl.col("bid_exchange") != pl.col("prev_bid_exchange")) &
            (pl.col("ask_exchange") == pl.col("prev_ask_exchange"))
        ).then(pl.lit("bid_ex_change"))

        .when(
            (pl.col("bid_exchange") == pl.col("prev_bid_exchange")) &
            (pl.col("ask_exchange") != pl.col("prev_ask_exchange"))
        ).then(pl.lit("ask_ex_change"))

        .when(
            (pl.col("bid_exchange") != pl.col("prev_bid_exchange")) &
            (pl.col("ask_exchange") != pl.col("prev_ask_exchange"))
        ).then(pl.lit("bid_ask_ex_change"))

        .when(same_price & (pl.col("bid_size") > pl.col("prev_bid_size")))
        .then(pl.lit("bid_size_up"))

        .when(same_price & (pl.col("bid_size") < pl.col("prev_bid_size")))
        .then(pl.lit("bid_size_down"))

        .when(same_price & (pl.col("ask_size") > pl.col("prev_ask_size")))
        .then(pl.lit("ask_size_up"))

        .when(same_price & (pl.col("ask_size") < pl.col("prev_ask_size")))
        .then(pl.lit("ask_size_down"))

        .when(
            (pl.col("ask_price") > pl.col("prev_ask_price")) &
            (pl.col("bid_price") == pl.col("prev_bid_price"))
        ).then(pl.lit("ask_up"))

        .when(
            (pl.col("ask_price") < pl.col("prev_ask_price")) &
            (pl.col("bid_price") == pl.col("prev_bid_price"))
        ).then(pl.lit("ask_down"))

        .when(
            (pl.col("ask_price") == pl.col("prev_ask_price")) &
            (pl.col("bid_price") > pl.col("prev_bid_price"))
        ).then(pl.lit("bid_up"))

        .when(
            (pl.col("ask_price") == pl.col("prev_ask_price")) &
            (pl.col("bid_price") < pl.col("prev_bid_price"))
        ).then(pl.lit("bid_down"))

        .when(
            (pl.col("ask_price") > pl.col("prev_ask_price")) &
            (pl.col("bid_price") > pl.col("prev_bid_price"))
        ).then(pl.lit("two_sided_price_up"))

        .when(
            (pl.col("ask_price") < pl.col("prev_ask_price")) &
            (pl.col("bid_price") < pl.col("prev_bid_price"))
        ).then(pl.lit("two_sided_price_down"))

        .when(
            (pl.col("ask_price") > pl.col("prev_ask_price")) &
            (pl.col("bid_price") < pl.col("prev_bid_price"))
        ).then(pl.lit("two_sided_spread_up"))

        .when(
            (pl.col("ask_price") < pl.col("prev_ask_price")) &
            (pl.col("bid_price") > pl.col("prev_bid_price"))
        ).then(pl.lit("two_sided_spread_down"))

        .otherwise(None)
    )

    q = q.with_columns(
        tick_type_expr.alias("tick_type")
    )


    return q


def build_dataset(trades, quotes):

    frame = (
        quotes

        # previous trade + last trade price
        .join_asof(
            trades.select(
                "ticker",
                pl.col("participant_timestamp").alias("trade_timestamp_prev"),
                pl.col("price").alias("last_trade_price"),
            ),
            left_on="participant_timestamp",
            right_on="trade_timestamp_prev",
            by="ticker",
            strategy="backward",
        )

        # next trade
        .join_asof(
            trades.select(
                "ticker",
                pl.col("participant_timestamp").alias("trade_timestamp_next"),
            ),
            left_on="participant_timestamp",
            right_on="trade_timestamp_next",
            by="ticker",
            strategy="forward",
        )
    )

    frame = frame.select(
        'ticker',
        'participant_timestamp',
        'tick_type',
        'trade_timestamp_prev',
        'last_trade_price',
        'trade_timestamp_next'
    )

    tick_enum = pl.Enum([
        "bid_ex_change",
        "ask_ex_change",
        "bid_ask_ex_change",
        "bid_size_up",
        "bid_size_down",
        "ask_size_up",
        "ask_size_down",
        "ask_up",
        "ask_down",
        "bid_up",
        "bid_down",
        "two_sided_price_up",
        "two_sided_price_down",
        "two_sided_spread_up",
        "two_sided_spread_down",
    ])

    frame = frame.with_columns(pl.col("tick_type").cast(tick_enum))


    return frame



def process_day(date, tickers, trades_path, quotes_path):

    trades, quotes = load_data(date, quotes_path=quotes_path, trades_path=trades_path)

    trades = trades.filter(pl.col("ticker").is_in(tickers))
    quotes = quotes.filter(pl.col("ticker").is_in(tickers))

    trades = trades.with_columns(pl.lit(True).alias("is_trade"))
    quotes = quotes.with_columns(pl.lit(False).alias("is_trade"))

    quotes = enrich_quotes(quotes)

    frame = build_dataset(trades, quotes)


    return frame.collect(engine="streaming")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Onboard Trades")
    parser.add_argument("--start_date", type=str)
    parser.add_argument("--end_date", type=str)
    parser.add_argument("--quote_data_dir", type=str)
    parser.add_argument("--trade_data_dir", type=str)
    parser.add_argument("--universe_path", type=str)
    parser.add_argument("--output_dir", type=str)

    args = parser.parse_args()

    date_range = pd.bdate_range(args.start_date, args.end_date)

    DATA_PATH = args.universe_path

    univ = pl.read_parquet(f"{args.universe_path}/universe.parquet")

    for date in tqdm(date_range):
        tickers = (
            univ
            .filter(pl.col("date") == date)
            .select("ticker")
            .unique()
            .to_series()
            .implode()
        )

        df = process_day(
            date=date.strftime("%Y%m%d"),
            tickers=tickers,
            trades_path=args.trade_data_dir,
            quotes_path=args.quote_data_dir
        )

        df.write_parquet(
            f"{args.output_dir}/{date.strftime('%Y%m%d')}.parquet"
        )