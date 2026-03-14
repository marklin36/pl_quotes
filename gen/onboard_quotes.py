import boto3
from botocore import args
from botocore.config import Config
import pandas as pd
from datetime import datetime, timedelta
import argparse
import botocore.exceptions
from tqdm import tqdm


API_KEY = "LqGXY_ISSIHm_1Ehu_oz6epZBWPrDlMv"
AWS_ACCES_KEY_ID = "dafcbb00-0329-4739-b1a8-7cd95eb014d0"
AWS_SECRET_ACCESS_KEY = "LqGXY_ISSIHm_1Ehu_oz6epZBWPrDlMv"

DATA_PATH = "/Volumes/Elements"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Onboard Quotes')
    parser.add_argument('--start_date', type=str,)
    parser.add_argument('--end_date', type=str,)
    parser.add_argument('--data_path', type=str, default=DATA_PATH,)

    args = parser.parse_args()

    date_range = pd.bdate_range(args.start_date, args.end_date)

    # Initialize a session using your credentials
    session = boto3.Session(
        aws_access_key_id=AWS_ACCES_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # Create a client with your session and specify the endpoint
    s3 = session.client(
        's3',
        endpoint_url='https://files.polygon.io',
        config=Config(signature_version='s3v4'),
    )

    failed_dates = []

    for date in tqdm(date_range):
        print("Downloading data for {}".format(date))
        date_str = date.strftime('%Y-%m-%d')
        year = date.strftime('%Y')
        month = date.strftime('%m')
        object_key = f"us_stocks_sip/quotes_v1/{year}/{month}/{date_str}.csv.gz"
        print(object_key)
        print(f"Downloading file '{object_key}")

        local_file_name = object_key.split('/')[-1].replace('-', '')  # e.g., '2025-06-12.csv.gz'
        local_file_path = args.data_path + "/data/quotes/raw/" + local_file_name
        print(local_file_path)

        # Download the file
        try:
            s3.download_file('flatfiles', object_key, local_file_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist for date: {}".format(date))
                failed_dates.append(date)
            else:
                raise

    print("Finished downloading data")
    print("Failed dates: {}".format(failed_dates))




