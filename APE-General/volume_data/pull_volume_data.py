import pandas as pd
import numpy as np
import boto3
import requests
import ast
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import pytz

KEY = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
s3 = boto3.client('s3')

def clean_csv(path):
    df_raw = pd.read_csv(path)
    # df_raw['tickers'].apply(lambda x: ast.literal_eval(x))
    # print(df_raw)
    breakoff_index = int(df_raw[df_raw['date'] == '2014-12-24'].index.values[0]) + 1
    df = df_raw.loc[breakoff_index:]
    # df['date'] = pd.to_datetime(df['date'])
    # print(df)
    idx = pd.date_range('2015-01-07', '2024-04-04').astype(str)
    df2 = pd.DataFrame(idx, columns = ['date'])
    df2['tickers'] = np.nan
    df_full = pd.merge(right= df2, left=df, how='outer', on = 'date')
    df_full.drop('tickers_y', axis=1, inplace=True)
    df_full.columns = ['date', 'tickers']
    df_full['tickers'] = df_full['tickers'].ffill()
    return df_full

def date_range(start_date, end_date, increment, period):
    result = []
    nxt = start_date
    delta = relativedelta(**{period:increment})
    while nxt <= end_date:
        result.append(nxt)
        nxt += delta
    return result



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
    """
    Sets up a requests Session with retries.
    
    Parameters:
    - retries: Number of retries before giving up. Default is 3.
    - backoff_factor: A factor to use for exponential backoff. Default is 0.3.
    - status_forcelist: A tuple of HTTP status codes that should trigger a retry. Default is (500, 502, 504).

    Returns:
    - A requests Session object with retry configuration.
    """
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
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/15/minute/{from_stamp}/{to_stamp}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
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

    return res_df

def create_15min_df(row):
    ##Perform 9-5
    year = int(row['date'].split('-')[0])
    month = int(row['date'].split('-')[1])
    day = int(row['date'].split('-')[2])
    start_date = datetime(year, month, day, 9, 0, 0)
    end_date = start_date + timedelta(hours=8)
    increment = 15
    period = 'minutes'
    times = date_range(start_date, end_date, increment, period)
    ticker_list = row['tickers'].split(',')
    df = pd.DataFrame(times, columns=['datetime'])

    # df.reindex(columns=ticker_list)
    # df = pd.DataFrame(columns = ticker_list)
    # df.insert(0, 'datetime', times)
    df.set_index('datetime', inplace=True)

    full_df = pull_volume_data(df, ticker_list, times, start_date, end_date)

        # for i in row['tickers'].split(','):
    # df.columns = ticker_list

    return full_df, year, month, day


def pull_volume_data(df, ticker_list, times, start_date, end_date):
    failed_list = []
    for i in ticker_list:
        try:
            ticker = str(i)
            res_df = polygon_volume_pull(start_date, end_date, ticker)
            # print(res_df)
            res_df.drop(labels = ['vw','o','c','h','l','t','n','time','hour','ticker'], axis=1, inplace=True)
            res_df.rename(columns = {'date': 'datetime', 'v':ticker}, inplace=True)
            res_df.set_index('datetime', inplace=True)
            # ticker_volume = pd.concat(res_df['date'], res_df['v'])
            # ticker_volume = res_df['v'].to_list()
            # print(ticker_volume)
            # df[ticker] = ticker_volume
            df = df.merge(res_df, left_index = True, right_index = True)
            # print(df)
        except Exception as e:
            print(e)
            failed_list.append(i)
            # df.drop(ticker, axis = 1, inplace = True)
            continue
    print("Failed volume pull here: " + failed_list)

    return df










if __name__ == '__main__':
    path = '/Users/diz/Documents/Projects/APE-Research/APE-General/volume_data/historical_sp500_tickers.csv'
    df = clean_csv(path)
    failed_list = []
    for i, row in df.iterrows():
        try:
            final_df, year, month, day = create_15min_df(row)
            s3.put_object(Body=final_df.to_csv(), Bucket="icarus-research-data", Key=f'historical_volume_data/{year}/{month}/{day}/volume_report.csv')
            print("Successful volume run for the date " + str(year) + '/' + str(month) + '/' + str(day))
        except Exception as e:
            print(e)
            print("Error in volume run for date " + str(year) + '/' + str(month) + '/' + str(day))
            failed_ticker = str(year) + '/' + str(month) + '/' + str(day)
            failed_list.append(failed_ticker)
    print("Failed script iteration pull here: " + failed_list)
    # df.to_csv('/Users/diz/Documents/Projects/APE-Research/APE-General/option_data/historical_sp500_tickers_daily_filled.csv')















    # df_full = df.set_index('date')
    # df.index = idx
    # df_full.reindex(pd.date_range('2015-01-07', '2024-04-04'))
            #    .rename_axis(['date'])
            #    .fillna(0)
            #    .reset_index())
    # idx = pd.date_range('2015-01-07', '2024-04-04').astype(str)
    # df.set_index('date', inplace= True)
    # print(df_full)
    # df.index = pd.DatetimeIndex(df['date'].index)
    # df_filled_dates = df.reindex(idx, fill_value = 0)
    # df.date = pd.to_datetime(df.date)
    # df_filled_dates = df.set_index('Date').resample('D')