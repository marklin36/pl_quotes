import polars as pl
import pandas as pd
from tqdm import tqdm
import argparse
from pathlib import Path


if __name__ == "__main__":

    schema = {
        "ticker": pl.Utf8,
        "conditions": pl.Utf8,
        "ask_exchange": pl.Int8,
        "ask_price": pl.Float32,
        "ask_size": pl.Int16,
        "bid_exchange": pl.Int8,
        "bid_price": pl.Float32,
        "bid_size": pl.Int16,
        "participant_timestamp": pl.Int64,
        "sequence_number": pl.Int64,
        "sip_timestamp": pl.Int64,
        "tape": pl.Int8,
        "trf_timestamp": pl.Int64,
        "indicators": pl.Utf8,
    }

    parser = argparse.ArgumentParser(description="Onboard Trades")
    parser.add_argument("--start_date", type=str)
    parser.add_argument("--end_date", type=str)
    parser.add_argument("--raw_data_dir", type=str)
    parser.add_argument("--clean_data_dir", type=str)

    args = parser.parse_args()

    date_range = pd.bdate_range(args.start_date, args.end_date)

    raw_data_dir = Path(args.raw_data_dir)
    clean_data_dir = Path(args.clean_data_dir)

    clean_data_dir.mkdir(parents=True, exist_ok=True)

    for date in tqdm(date_range):

        date_str = date.strftime("%Y%m%d")
        input_path = raw_data_dir / f"{date_str}.csv.gz"
        output_path = clean_data_dir / f"{date_str}.parquet"

        if not input_path.exists():
            print(f"File {date_str} not found.")
            continue

        try:

            df = (
                pl.scan_csv(
                    input_path,
                    infer_schema_length=0,
                    ignore_errors=True,
                    null_values=["", " ", "NA"],
                )
                .with_columns([
                    pl.col("price").cast(pl.Float32, strict=False),
                    pl.col("size").cast(pl.Int32, strict=False),
                    pl.col("exchange").cast(pl.Int8, strict=False),
                    pl.col("participant_timestamp").cast(pl.Int64, strict=False),
                    pl.col("sequence_number").cast(pl.Int64, strict=False),
                    pl.col("sip_timestamp").cast(pl.Int64, strict=False),
                    pl.col("tape").cast(pl.Int8, strict=False),
                    pl.col("trf_timestamp").cast(pl.Int64, strict=False),
                    pl.col("trf_id").cast(pl.Categorical, strict=False),
                ])
            )


            df.sink_parquet(output_path)

            #df.collect(streaming=True).write_parquet(output_path)

        except Exception as e:
            print(f"Error processing {date_str}: {e}")