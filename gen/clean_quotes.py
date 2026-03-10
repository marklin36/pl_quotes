import numpy as np
import pandas as pd
import glob
from tqdm import tqdm
import argparse

# TODO: run this script

START_DATE = pd.to_datetime('2022-02-24')
END_DATE = pd.to_datetime('2022-06-30')
RAW_DATA_DIR = '/Volumes/Elements/data/quotes/raw'
CLEAN_DATA_DIR = '/Volumes/Elements2/data/quotes/clean'
#CLEAN_DATA_DIR = '/Volumes/Elements/quotes/clean/'
# RAW_DATA_DIR = '/media/kolaszewski/Elements/data/quotes/raw'
# CLEAN_DATA_DIR = '/media/kolaszewski/Elements/data/quotes/clean'

if __name__ == "__main__":
    dtypes = {
        'ticker': str,
        'conditions': str,
        'ask_exchange': np.int8,
        'ask_price': np.float32,
        'ask_size': np.int16,
        'bid_exchange': np.int8,
        'bid_price': np.float32,
        'bid_size': np.int16,
        'participant_timestamp': np.int64,
        'sequence_number': np.int64,
        'sip_timestamp': np.int64,
        'tape': np.int8,
        'trf_timestamp': np.int64,
        'indicators': str,
    }

    parser = argparse.ArgumentParser(description='Onboard Trades')
    parser.add_argument('--start_date', type=str, )
    parser.add_argument('--end_date', type=str, )
    parser.add_argument('--raw_data_dir', type=str, default=RAW_DATA_DIR)
    parser.add_argument('--clean_data_dir', type=str, default=CLEAN_DATA_DIR)


    args = parser.parse_args()

    date_range = pd.bdate_range(args.start_date, args.end_date)
    raw_data_dir = args.raw_data_dir
    clean_data_dir = args.clean_data_dir

    for date in tqdm(date_range):
        try:
            df = pd.read_csv(f"{raw_data_dir}/{date.strftime('%Y%m%d')}.csv.gz")

            df['indicators'] = df['indicators'].fillna(0)
            df['ask_size'] = df['ask_size'].fillna(0)
            df['bid_size'] = df['bid_size'].fillna(0)

            df = df.astype(dtypes)

            df.to_parquet(f"{clean_data_dir}/{date.strftime('%Y%m%d')}.parquet")

        except FileNotFoundError as e:
            print(f"File {date.strftime('%Y%m%d')} not found.")



