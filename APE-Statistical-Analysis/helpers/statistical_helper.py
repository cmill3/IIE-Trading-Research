import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import pytz
import json
import ta_formulas as ta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import boto3
import ast

def convert_timestamp_est(timestamp):
    # Create a UTC datetime object from the timestamp
    utc_datetime = datetime.fromtimestamp(timestamp).replace(tzinfo=pytz.utc)
    # Define the EST timezone
    est_timezone = pytz.timezone('America/New_York')
    # Convert the UTC datetime to EST
    est_datetime = utc_datetime.astimezone(est_timezone)
    return est_datetime

def polygon_call_stocks(contract, from_stamp, to_stamp, multiplier, timespan, open_date):
    trading_hours = ['09:30:00', '16:00:00']
    try:
        payload={}
        headers = {}
        url = f"https://api.polygon.io/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"
        response = requests.request("GET", url, headers=headers, data=payload)
        res_df = pd.DataFrame(json.loads(response.text)['results'])
        res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
        res_df['date'] = res_df['t'].apply(lambda x: convert_timestamp_est(x))
        res_df['year'] = res_df['date'].apply(lambda x: x.year)
        res_df['month'] = res_df['date'].apply(lambda x: x.month)
        res_df['time'] = res_df['date'].apply(lambda x: x.time())
        res_df['hour'] = res_df['date'].apply(lambda x: x.hour)
        res_df['minute'] = res_df['date'].apply(lambda x: x.minute)
        res_df['dt'] = res_df['date'].apply(lambda x: datetime(x.year, x.month, x.day,x.hour,x.minute))
        res_df = res_df[(res_df['time'] >= datetime.strptime(trading_hours[0], '%H:%M:%S').time()) & (res_df['time'] <= datetime.strptime(trading_hours[1], '%H:%M:%S').time())]
        res_df = res_df[res_df['dt'] >= datetime.strptime(open_date, '%Y-%m-%d %H:%M')]

        res_df.drop(['vw', 'h', 'l', 'c', 'n', 't','date', 'year','month', 'time', 'hour', 'minute'], axis = 1, inplace = True)
        res_df.reset_index(inplace=True)
        res_df.rename(columns = {'v': 'underlying_volume', 'o': 'underlying_price'}, inplace=True)
        res_df['symbol'] = contract
        res_df = res_df[['symbol', 'dt', 'underlying_price', 'underlying_volume']]

            
        return res_df
    except Exception as e:  
        print(e)
        return pd.DataFrame()

def polygon_call_options(contract, from_stamp, to_stamp, multiplier, timespan, open_date):
    trading_hours = ['09:30:00', '16:00:00']
    try:
        payload={}
        headers = {}
        url = f"https://api.polygon.io/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"
        response = requests.request("GET", url, headers=headers, data=payload)
        res_df = pd.DataFrame(json.loads(response.text)['results'])
        res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
        res_df['date'] = res_df['t'].apply(lambda x: convert_timestamp_est(x))
        res_df['year'] = res_df['date'].apply(lambda x: x.year)
        res_df['month'] = res_df['date'].apply(lambda x: x.month)
        res_df['time'] = res_df['date'].apply(lambda x: x.time())
        res_df['hour'] = res_df['date'].apply(lambda x: x.hour)
        res_df['minute'] = res_df['date'].apply(lambda x: x.minute)
        res_df['dt'] = res_df['date'].apply(lambda x: datetime(x.year, x.month, x.day,x.hour,x.minute))
        res_df = res_df[(res_df['time'] >= datetime.strptime(trading_hours[0], '%H:%M:%S').time()) & (res_df['time'] <= datetime.strptime(trading_hours[1], '%H:%M:%S').time())]
        res_df = res_df[res_df['dt'] >= datetime.strptime(open_date, '%Y-%m-%d %H:%M')]

        res_df.drop(['vw', 'h', 'l', 'c', 'n', 't','date', 'year','month', 'time', 'hour', 'minute'], axis = 1, inplace = True)
        res_df.reset_index(inplace=True)
        res_df.rename(columns = {'v': 'option_volume', 'o': 'option_price'}, inplace=True)
        res_df['option_contract'] = contract
        res_df = res_df[['option_contract', 'dt', 'option_price', 'option_volume']]
            
        return res_df
    except Exception as e:  
        print(e)
        return pd.DataFrame()


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
    
def stat_window_creator(symbol, from_stamp, to_stamp, timespan, multiplier):
    trading_hours = [9,10,11,12,13,14,15]
    
    from_dt = datetime.strptime(from_stamp, '%Y-%m-%d')
    new_stamp = from_dt - timedelta(days = 1)
    from_stamp = new_stamp.strftime('%Y-%m-%d')

    data = []
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"
    response = execute_polygon_call(url)
    response_data = json.loads(response.text)
    results = response_data['results']
    results_df = pd.DataFrame(results)
    results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
    results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
    results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
    results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
    results_df['day'] = results_df['date'].apply(lambda x: x.day)
    results_df['month'] = results_df['date'].apply(lambda x: x.month)
    results_df['symbol'] = symbol
    trimmed_df = results_df.loc[results_df['hour'].isin(trading_hours)]
    filtered_df = trimmed_df.loc[~((trimmed_df['hour'] == 9) & (trimmed_df['minute'] < 30))]

    return filtered_df

