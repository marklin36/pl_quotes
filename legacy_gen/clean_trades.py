import pandas as pd
import numpy as np
from tqdm import tqdm
import time

# TODO: RUN it
START_DATE = pd.to_datetime('2022-01-01')
END_DATE = pd.to_datetime('2022-06-30')
RAW_DATA_DIR = '/Volumes/Elements2/data/trades/raw/'
CLEAN_DATA_DIR = '/Volumes/Elements2/data/trades/clean/'

if __name__ == '__main__':
    #print("sleeping")
    #time.sleep(500)
    dtypes = {
        'ticker': str,
        'conditions': str,
        'correction': np.int8,
        'exchange': np.int8,
        'size': np.int32,
        'participant_timestamp': np.int64,
        'price': np.float32,
        'sequence_number': np.int64,
        'sip_timestamp': np.int64,
        'tape': np.int8,
        'trf_id': np.int64,
    }

    date_range = pd.bdate_range(START_DATE, END_DATE)

    for date in tqdm(date_range):
        print(date)
        try:
            df = pd.read_csv(f"{RAW_DATA_DIR}/{date.strftime('%Y%m%d')}.csv.gz")

            df = df.astype(dtypes)
            df = df.drop('id', axis=1)
            df.to_parquet(f"{CLEAN_DATA_DIR}/{date.strftime('%Y%m%d')}.parquet")
        except FileNotFoundError as e:
            print(f"File {date.strftime('%Y%m%d')} not found.")

