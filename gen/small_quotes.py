import polars as pl
import pandas as pd
from tqdm import tqdm
import argparse
from pathlib import Path


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Onboard Trades")
    parser.add_argument("--start_date", type=str)
    parser.add_argument("--end_date", type=str)
    parser.add_argument("--raw_data_dir", type=str)
    parser.add_argument("--clean_data_dir", type=str)
    parser.add_argument("--universe_path", type=str)

    args = parser.parse_args()

    date_range = pd.bdate_range(args.start_date, args.end_date)

    raw_data_dir = Path(args.raw_data_dir)
    clean_data_dir = Path(args.clean_data_dir)
    universe_path = Path(args.universe_path)

    clean_data_dir.mkdir(parents=True, exist_ok=True)

    for date in tqdm(date_range):

        date_str = date.strftime("%Y%m%d")
        input_path = raw_data_dir / f"{date_str}.parquet"
        output_path = clean_data_dir / f"{date_str}.parquet"

        if not input_path.exists():
            print(f"File {date_str} not found.")
            continue

        try:
            df = pl.read_parquet(input_path)
            univ = pl.read_parquet(universe_path)

            tickers = (
                univ
                .filter(pl.col("date") == date)
                .select("ticker")
                .unique()
                .to_series()
                .implode()
            )

            df = df.filter(pl.col('ticker').is_in(tickers))
            df.write_parquet(output_path)
        except Exception as e:
            print(f"Error processing {date_str}: {e}")