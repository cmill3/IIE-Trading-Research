from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pytz

YEAR_CONFIG = {
  "twenty0": {
        "months": [
        ["2020-01-06", "2020-01-13", "2020-01-20", "2020-01-27"],
        ["2020-02-03", "2020-02-10", "2020-02-17", "2020-02-24"],
        ["2020-03-02", "2020-03-09", "2020-03-16", "2020-03-23", "2020-03-30"],
        ["2020-04-06", "2020-04-13", "2020-04-20", "2020-04-27"],
        ["2020-05-04", "2020-05-11", "2020-05-18", "2020-05-25"],
        ["2020-06-01", "2020-06-08", "2020-06-15", "2020-06-22", "2020-06-29"],
        ["2020-07-06", "2020-07-13", "2020-07-20", "2020-07-27"],
        ["2020-08-03", "2020-08-10", "2020-08-17", "2020-08-24", "2020-08-31"],
        ["2020-09-07", "2020-09-14", "2020-09-21", "2020-09-28"],
        ["2020-10-05", "2020-10-12", "2020-10-19", "2020-10-26"],
        ["2020-11-02", "2020-11-09", "2020-11-16", "2020-11-23", "2020-11-30"],
        ["2020-12-07", "2020-12-14", "2020-12-21", "2020-12-28"]
        ],
        "all_files": [
        "2020-01-06", "2020-01-13", "2020-01-20", "2020-01-27",
        "2020-02-03", "2020-02-10", "2020-02-17", "2020-02-24",
        "2020-03-02", "2020-03-09", "2020-03-16", "2020-03-23", "2020-03-30",
        "2020-04-06", "2020-04-13", "2020-04-20", "2020-04-27",
        "2020-05-04", "2020-05-11", "2020-05-18", "2020-05-25",
        "2020-06-01", "2020-06-08", "2020-06-15", "2020-06-22", "2020-06-29",
        "2020-07-06", "2020-07-13", "2020-07-20", "2020-07-27",
        "2020-08-03", "2020-08-10", "2020-08-17", "2020-08-24", "2020-08-31",
        "2020-09-07", "2020-09-14", "2020-09-21", "2020-09-28",
        "2020-10-05", "2020-10-12", "2020-10-19", "2020-10-26",
        "2020-11-02", "2020-11-09", "2020-11-16", "2020-11-23", "2020-11-30",
        "2020-12-07", "2020-12-14", "2020-12-21", "2020-12-28"
        ],
        "year": "20"
    },
    "twenty1": {
        "months":
            [
            ['2021-01-04', '2021-01-11', '2021-01-18', '2021-01-25'],
            ['2021-02-01', '2021-02-08', '2021-02-15', '2021-02-22'],
            ['2021-03-01', '2021-03-08', '2021-03-15','2021-03-22', '2021-03-29'],
            ['2021-04-05', '2021-04-12', '2021-04-19', '2021-04-26'],
            [ '2021-05-03', '2021-05-10', '2021-05-17', 
            '2021-05-24', '2021-05-31'],
            ['2021-06-07', '2021-06-14', '2021-06-21', '2021-06-28'],
            ['2021-07-05', '2021-07-12','2021-07-19', '2021-07-26'],
            ['2021-08-02', '2021-08-09', '2021-08-16', '2021-08-23', '2021-08-30'],
            ['2021-09-06', '2021-09-13', '2021-09-20', '2021-09-27'],
            ['2021-10-04', '2021-10-11', '2021-10-18', '2021-10-25'],
            ['2021-11-01', '2021-11-08', '2021-11-15', '2021-11-22', '2021-11-29'],
            ['2021-12-06', '2021-12-13', '2021-12-20', '2021-12-27'],
            ],
        "all_files":
        [
                '2021-01-04', '2021-01-11', '2021-01-18', '2021-01-25', '2021-02-01', 
                '2021-02-08', '2021-02-15', '2021-02-22', '2021-03-01', '2021-03-08', 
                '2021-03-15','2021-03-22', '2021-03-29', '2021-04-05', '2021-04-12',
                '2021-04-19', '2021-04-26', '2021-05-03', '2021-05-10', '2021-05-17',
                '2021-05-24', '2021-05-31', '2021-06-07', '2021-06-14', '2021-06-21',
                '2021-06-28', '2021-07-05', '2021-07-12', '2021-07-19', '2021-07-26',
                '2021-08-02', '2021-08-09', '2021-08-16', '2021-08-23', '2021-08-30',
                '2021-09-06', '2021-09-13', '2021-09-20', '2021-09-27', '2021-10-04',
                '2021-10-11', '2021-10-18', '2021-10-25', '2021-11-01', '2021-11-08',
                '2021-11-15', '2021-11-22', '2021-11-29', '2021-12-06', '2021-12-13',
                '2021-12-20', '2021-12-27'
        ],
        "year": "21"
    },
    "twenty2": {
        "months":
            [
            ['2022-01-03', '2022-01-10', '2022-01-17', '2022-01-24', 
            '2022-01-31'],
            ['2022-02-07', '2022-02-14', '2022-02-21', '2022-02-28'],
            ['2022-03-07', '2022-03-14', '2022-03-21','2022-03-28'],
            ['2022-04-04', '2022-04-11', '2022-04-18', '2022-04-25'],
            ['2022-05-02', '2022-05-09', '2022-05-16', 
            '2022-05-23', '2022-05-30'],
            ['2022-06-06', '2022-06-13', '2022-06-20', '2022-06-27'],
            ['2022-07-04', '2022-07-11','2022-07-18', '2022-07-25'],
            ['2022-08-01', '2022-08-08', '2022-08-15', '2022-08-22', '2022-08-29'],
            ['2022-09-05', '2022-09-12', '2022-09-19', '2022-09-26'],
            ['2022-10-03', '2022-10-10', '2022-10-17', '2022-10-24', '2022-10-31'],
            ['2022-11-07', '2022-11-14', '2022-11-21', '2022-11-28'],
            ['2022-12-05', '2022-12-12', '2022-12-19', '2022-12-26'],
            ],
        "all_files":
        [
                '2022-01-03', '2022-01-10', '2022-01-17', '2022-01-24','2022-01-31', 
                '2022-02-07', '2022-02-14', '2022-02-21', '2022-02-28', '2022-03-07', 
                '2022-03-14', '2022-03-21', '2022-03-28', '2022-04-04', '2022-04-11',
                '2022-04-18', '2022-04-25', '2022-05-02', '2022-05-09', '2022-05-16',
                '2022-05-23', '2022-05-30', '2022-06-06', '2022-06-13', '2022-06-20',
                '2022-06-27', '2022-07-04', '2022-07-11', '2022-07-18', '2022-07-25',
                '2022-08-01', '2022-08-08', '2022-08-15', '2022-08-22', '2022-08-29',
                '2022-09-05', '2022-09-12', '2022-09-19', '2022-09-26', '2022-10-03',
                '2022-10-10', '2022-10-17', '2022-10-24', '2022-10-31', '2022-11-07',
                '2022-11-14', '2022-11-21', '2022-11-28', '2022-12-05', '2022-12-12',
                '2022-12-19', '2022-12-26'
        ],
        "year": "22"
    },
    "twenty3": {
        "months":
            [
            ['2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
            '2023-01-30'],
            ['2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27'],
            ['2023-03-06', '2023-03-13', '2023-03-20','2023-03-27'],
            ['2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24'],
            [ '2023-05-01', '2023-05-08', '2023-05-15', 
            '2023-05-22', '2023-05-29'],
            ['2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26'],
            ['2023-07-03', '2023-07-10','2023-07-17', '2023-07-24', '2023-07-31'],
            ['2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28'],
            ['2023-09-04', '2023-09-11', '2023-09-18', '2023-09-25'],
            ['2023-10-02', '2023-10-09', '2023-10-16', '2023-10-23', '2023-10-30'],
            ['2023-11-06', '2023-11-13', '2023-11-20', '2023-11-27'],
            ['2023-12-04', '2023-12-11', '2023-12-18'],
            ],
        "all_files":
        [
                '2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
                '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
                '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
                '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26', '2023-07-03', '2023-07-10', 
                '2023-07-17', '2023-07-24', '2023-07-31', '2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28', '2023-09-04', 
                '2023-09-11', '2023-09-18', '2023-09-25', '2023-10-02', '2023-10-09', '2023-10-16', '2023-10-23', '2023-10-30',
                '2023-11-06', '2023-11-13', '2023-11-20', '2023-11-27', '2023-12-04', '2023-12-11', '2023-12-18', '2023-12-25'
        ],
        "year": "23"
    },
    "twenty4": {
        "months":
            [
            ['2024-01-01', '2024-01-08', '2024-01-15', '2024-01-22', 
            '2024-01-29'],
            ['2024-02-05', '2024-02-12', '2024-02-19', '2024-02-26'],
            ['2024-03-04', '2024-03-11', '2024-03-18','2024-03-25'],
            ['2024-04-01', '2024-04-08', '2024-04-15', '2024-04-22', '2024-04-29'],
            [ '2024-05-06', '2024-05-13', '2024-05-20', 
            '2024-05-27'],
            ['2024-06-03', '2024-06-10', '2024-06-17', '2024-06-24'],
            ['2024-07-01', '2024-07-08','2024-07-15', '2024-07-22', '2024-07-29'],
            ['2024-08-05', '2024-08-12', '2024-08-19', '2024-08-26'],
            ['2024-09-02', '2024-09-09', '2024-09-16', '2024-09-23', '2024-09-30'],
            ['2024-10-07', '2024-10-14', '2024-10-21', '2024-10-28'],
            ['2024-11-04', '2024-11-11', '2024-11-18', '2024-11-25'],
            ['2024-12-02', '2024-12-09', '2024-12-16', '2024-12-23', '2024-12-30'],
            ],
        "all_files":[
                '2024-01-01', '2024-01-08', '2024-01-15', '2024-01-22', 
                '2024-01-29', '2024-02-05', '2024-02-12', '2024-02-19', '2024-02-26', '2024-03-04', '2024-03-11', '2024-03-18', 
                '2024-03-25', '2024-04-01', '2024-04-08', '2024-04-15', '2024-04-22', '2024-04-29', '2024-05-06', '2024-05-13', 
                '2024-05-20', '2024-05-27', '2024-06-03', '2024-06-10', '2024-06-17', '2024-06-24', '2024-07-01', '2024-07-08', 
                '2024-07-15', '2024-07-22', '2024-07-29', '2024-08-05', '2024-08-12', '2024-08-19', '2024-08-26', '2024-09-02', 
                '2024-09-09', '2024-09-16', '2024-09-23', '2024-09-30', '2024-10-07', '2024-10-14', '2024-10-21', '2024-10-28',
                '2024-11-04', '2024-11-11', '2024-11-18', '2024-11-25', '2024-12-02', '2024-12-09', '2024-12-16', '2024-12-23', '2024-12-30'    
        ],
        "year": "24"
    },
    "twenty0": {
        "months": [
        ["2020-01-06", "2020-01-13", "2020-01-20", "2020-01-27"],
        ["2020-02-03", "2020-02-10", "2020-02-17", "2020-02-24"],
        ["2020-03-02", "2020-03-09", "2020-03-16", "2020-03-23", "2020-03-30"],
        ["2020-04-06", "2020-04-13", "2020-04-20", "2020-04-27"],
        ["2020-05-04", "2020-05-11", "2020-05-18", "2020-05-25"],
        ["2020-06-01", "2020-06-08", "2020-06-15", "2020-06-22", "2020-06-29"],
        ["2020-07-06", "2020-07-13", "2020-07-20", "2020-07-27"],
        ["2020-08-03", "2020-08-10", "2020-08-17", "2020-08-24", "2020-08-31"],
        ["2020-09-07", "2020-09-14", "2020-09-21", "2020-09-28"],
        ["2020-10-05", "2020-10-12", "2020-10-19", "2020-10-26"],
        ["2020-11-02", "2020-11-09", "2020-11-16", "2020-11-23", "2020-11-30"],
        ["2020-12-07", "2020-12-14", "2020-12-21", "2020-12-28"]
        ],
        "all_files": [
        "2020-01-06", "2020-01-13", "2020-01-20", "2020-01-27",
        "2020-02-03", "2020-02-10", "2020-02-17", "2020-02-24",
        "2020-03-02", "2020-03-09", "2020-03-16", "2020-03-23", "2020-03-30",
        "2020-04-06", "2020-04-13", "2020-04-20", "2020-04-27",
        "2020-05-04", "2020-05-11", "2020-05-18", "2020-05-25",
        "2020-06-01", "2020-06-08", "2020-06-15", "2020-06-22", "2020-06-29",
        "2020-07-06", "2020-07-13", "2020-07-20", "2020-07-27",
        "2020-08-03", "2020-08-10", "2020-08-17", "2020-08-24", "2020-08-31",
        "2020-09-07", "2020-09-14", "2020-09-21", "2020-09-28",
        "2020-10-05", "2020-10-12", "2020-10-19", "2020-10-26",
        "2020-11-02", "2020-11-09", "2020-11-16", "2020-11-23", "2020-11-30",
        "2020-12-07", "2020-12-14", "2020-12-21", "2020-12-28"
        ],
        "year": "20"
    }
}

def calculate_contract_price_statistics(row,alert_row):
    try:
        option_price_data = polygon_stockdata_inv(row['contract_name'], alert_row['dt'], alert_row['dt'])
        alert_date = datetime(int(alert_row['year']), int(alert_row['month']), int(alert_row['day_of_month']),int(alert_row['hour']),int(alert_row['minute']))
        option_price_data['compare_dt'] = option_price_data.apply(lambda x: datetime(int(alert_row['year']), int(alert_row['month']), int(alert_row['day_of_month']),int(x['hour']),int(x['minute'])), axis=1)
        ## filter out dates before alert date
        option_price_data = option_price_data[option_price_data['compare_dt'] >= alert_date]
        ## create time_diff column which is minutes diff of option price data['date'] and alert date
        option_price_data['time_diff'] = option_price_data['compare_dt'].apply(lambda x: (x - alert_date).total_seconds() / 60)
        time_horizion_df = option_price_data[option_price_data['time_diff'] <= 120]
        open_price = time_horizion_df.iloc[0]['o']
        time_horizion_df = time_horizion_df[time_horizion_df['time_diff'] > 0].reset_index(drop=True)
        time_horizion_df["price_appreciation"] = time_horizion_df['h'] - open_price
        median_price_appreciation = time_horizion_df['price_appreciation'].median()
        max_idx = time_horizion_df['h'].idxmax()
        min_idx = time_horizion_df['l'].idxmin()
        max_data = time_horizion_df.iloc[max_idx]
        min_data = time_horizion_df.iloc[min_idx]
        max_time_diff = max_data['time_diff']
        min_time_diff = min_data['time_diff']
        max_price = max_data['h']
        min_price = min_data['l']
        return pd.Series([max_price, min_price, max_time_diff, min_time_diff, open_price, median_price_appreciation])
    except Exception as e:
        # print(e)
        # print(row['contract_name'])
        # print(alert_row)
        return pd.Series([None, None, None, None, None])


def extract_strike_price(contract_code, option_type):
    # Find the last occurrence of 'C' or 'P' in the string
    # option_type_index = max(contract_code.rfind('C'), contract_code.rfind('P'))
    if option_type == "C":
        option_type_index = contract_code.rfind('C')
    elif option_type == "P":
        option_type_index = contract_code.rfind('P')
    
    # Extract the strike price portion (last 8 characters after C or P)
    strike_price_str = contract_code[option_type_index + 1:]
    expiry = contract_code[option_type_index - 6:option_type_index]
    
    # Convert to float and divide by 1000 to get the actual strike price
    strike_price = float(strike_price_str) / 1000
    
    return pd.Series([strike_price, expiry])

def polygon_stockdata_inv(symbol, from_date, to_date):

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/10/minute/{from_date}/{to_date}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: convert_timestamp_est(x))
    stock_df['time'] = stock_df['date'].apply(lambda x: x.time())
    stock_df['hour'] = stock_df['date'].apply(lambda x: x.hour)
    stock_df['minute'] = stock_df['date'].apply(lambda x: x.minute)
    stock_df['ticker'] = symbol

    stock_df = stock_df[stock_df['hour'] < 16]
    stock_df = stock_df.loc[stock_df['time'] >= datetime.strptime("09:30:00", "%H:%M:%S").time()]
    return stock_df

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

def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est

def stock_aggs(symbol,from_date, to_date):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/5/minute/{from_date}/{to_date}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"
    response = execute_polygon_call(url)

    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: convert_timestamp_est(x))
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


