from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pytz
import helpers.helper as helper

KEY = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"

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

def polygon_optiondata(options_ticker, from_date, to_date,frequency):
    #This is for option data
    if from_date == None:
        from_date = to_date - timedelta(days=1)
        from_stamp = int(from_date.timestamp() * 1000)
        to_stamp = int(to_date.timestamp() * 1000)
    else:
        from_stamp = int(from_date.timestamp() * 1000)
        to_stamp = int(to_date.timestamp() * 1000)

    url = f"https://api.polygon.io/v2/aggs/ticker/{options_ticker}/range/{frequency}/minute/{from_stamp}/{to_stamp}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)
    response_data = json.loads(response.text)

    res_option_df = pd.DataFrame(response_data['results'])
    res_option_df['t'] = res_option_df['t'].apply(lambda x: int(x/1000))
    res_option_df['date'] = res_option_df['t'].apply(lambda x: helper.convert_timestamp_est(x))
    res_option_df['time'] = res_option_df['date'].apply(lambda x: x.time())
    res_option_df['hour'] = res_option_df['date'].apply(lambda x: x.hour)
    res_option_df['ticker'] = options_ticker

    res_option_df =res_option_df[res_option_df['hour'] < 16]
    res_option_df =res_option_df.loc[res_option_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return res_option_df

def polygon_stockdata_inv(symbol, from_date, to_date,frequency, time_span):
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{frequency}/{time_span}/{from_stamp}/{to_stamp}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: helper.convert_timestamp_est(x))
    stock_df['time'] = stock_df['date'].apply(lambda x: x.time())
    stock_df['hour'] = stock_df['date'].apply(lambda x: x.hour)
    stock_df['minute'] = stock_df['date'].apply(lambda x: x.minute)
    stock_df['ticker'] = symbol

    stock_df = stock_df[stock_df['hour'] < 16]
    stock_df = stock_df.loc[stock_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return stock_df

def stock_aggs(symbol,from_date, to_date):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/5/minute/{from_date}/{to_date}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: helper.convert_timestamp_est(x))
    stock_df['time'] = stock_df['date'].apply(lambda x: x.time())
    stock_df['hour'] = stock_df['date'].apply(lambda x: x.hour)
    stock_df['minute'] = stock_df['date'].apply(lambda x: x.minute)
    stock_df['ticker'] = symbol

    stock_df = stock_df[stock_df['hour'] < 16]
    stock_df = stock_df.loc[stock_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return stock_df


def get_last_price(row):
    aggs = stock_aggs(row['symbol'], row['date'], row['date'])
    agg = aggs.loc[aggs['hour'] == row['hour']]
    agg = agg.loc[agg['minute'] == row['minute']]   
    return agg['o'].values[0]