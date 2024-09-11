import pandas as pd
import numpy as np
import boto3
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import pytz
import pandas_market_calendars as mcal

nyse = mcal.get_calendar('NYSE')
holidays = nyse.regular_holidays
market_holidays = holidays.holidays()

KEY = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
s3 = boto3.client('s3')


def date_range(start_date, end_date, increment, period):
    result = []
    nxt = start_date
    delta = relativedelta(**{period:increment})
    while nxt <= end_date:
        result.append(nxt)
        nxt += delta
    return result

def create_s3_path(date_str):
    year = date_str.split('-')[0]
    month = date_str.split('-')[1]
    day = date_str.split('-')[2]
    path = f'historical_volume_data/{year}/{month}/{day}/volume_report.csv'
    return path


def pull_s3_csv(bucket, prefix):
    response = s3.get_object(Bucket = bucket, Key = prefix)
    df = pd.read_csv(response['Body'])
    return df

def put_historical_s3(date, df):
    date_key = date.replace('-', '/')
    s3.put_object(Body=df.to_csv(), Bucket="icarus-research-data", Key=f'top_50_historical_volume_data/{date_key}/volume_report.csv')
    return print("Top 50 upload for date " + date_key + " is successful.")

def pull_monthly_sp500_constituents_s3(path, bucket):
    date = datetime.today().strftime('%Y/%m')
    new_path = path + '/' + date + '/'
    keys = s3.list_objects(Bucket=bucket,Prefix=f"{new_path}")["Contents"]
    sp_object = keys[-1]
    key = sp_object['Key']
    print(key)
    dataset = s3.get_object(Bucket=bucket,Key=f"{key}")
    df = pd.read_csv(dataset.get("Body"))
    df.drop(columns=['Unnamed: 0'],inplace=True)
    df.reset_index(drop=True)
    return df

class CustomRetry(Retry):
    def is_retry(self, method, status_code, has_retry_after=False):
        """ Return True if we should retry the request, otherwise False. """
        if status_code != 200:
            return True
        return super().is_retry(method, status_code, has_retry_after)
    
def setup_session_retries(
    retries: int = 3,
    backoff_factor: float = 0.05,
    status_forcelist: tuple = (500, 502, 504),
):
    retry_strategy = CustomRetry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def execute_polygon_call(url):
    session = setup_session_retries()
    response = session.request("GET", url, headers={}, data={})
    return response 

def convert_timestamp_est(timestamp):
    # Create a UTC datetime object from the timestamp
    utc_datetime = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)
    # Define the EST timezone
    est_timezone = pytz.timezone('America/New_York')
    # Convert the UTC datetime to EST
    est_datetime = utc_datetime.astimezone(est_timezone).replace(tzinfo=None)
    return est_datetime

def polygon_volume_pull(from_date, to_date, ticker):
    from_str = from_date.strftime('%Y-%m-%d')
    to_str = to_date.strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/15/minute/{from_str}/{to_str}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)
    # print(response)
    response_data = json.loads(response.text)
    # print(response_data)
    res_df = pd.DataFrame(response_data['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: convert_timestamp_est(x))
    res_df['time'] = res_df['date'].apply(lambda x: x.time())
    res_df['hour'] = res_df['date'].apply(lambda x: x.hour)
    res_df['ticker'] = ticker

    return res_df, from_str


def pull_volume_data(df, ticker_list, times, start_date, end_date):
    failed_list = []
    for i in ticker_list:
        try:
            ticker = str(i)
            res_df, from_str = polygon_volume_pull(start_date, end_date, ticker)
            res_df = res_df[['date','v']]
            res_df.rename(columns = {'date': 'datetime', 'v':ticker}, inplace=True)
            res_df.set_index('datetime', inplace=True)
            df = df.merge(res_df, left_index = True, right_index = True)

        except Exception as e:
            failed_list.append(i)
            continue
    print(f"Failed volume pull here: {failed_list} on date {from_str}")

    return df, from_str

