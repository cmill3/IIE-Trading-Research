from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# warnings.filterwarnings("ignore", category=FutureWarning)
# warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=pd.core.common.SettingWithCopyWarning)


def get_business_days(transaction_date, current_date):
    """
    Returns the number of business days (excluding weekends) between two dates. For now we
    aren't considering market holidays because that is how the training data was generated.
    """
    
    # transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%d %H:%M")
    # current_date = datetime.strptime(current_date_str, "%Y-%m-%d %H:%M")
    # We aren't inclusive of the transaction date
    days = (current_date - transaction_date).days 
    complete_weeks = days // 7
    remaining_days = days % 7

    # Calculate the number of weekend days in the complete weeks
    weekends = complete_weeks * 2

    # Adjust for the remaining days
    if remaining_days:
        start_weekday = transaction_date.weekday()
        end_weekday = current_date.weekday()

        if start_weekday <= end_weekday:
            if start_weekday <= 5 and end_weekday >= 5:
                weekends += 2  # Include both Saturdays and Sundays
            elif start_weekday <= 4 and end_weekday >= 4:
                weekends += 1  # Include only Saturdays
        else:
            if start_weekday <= 5:
                weekends += 1  # Include Saturday of the first week

    business_days = days - weekends
    return business_days 

def calculate_floor_pct_call(row):
   from_stamp = row['order_transaction_date'].split('T')[0]
   time_stamp = datetime.strptime(row['order_transaction_date'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() - timedelta(hours=4).total_seconds()
   prices = polygon_call_stocks(row['underlying_symbol'], from_stamp, current_date, "1", "hour")
   trimmed_df = prices.loc[prices['t'] > time_stamp]
   high_price = trimmed_df['h'].max()
   low_price = trimmed_df['l'].min()
   if row['trading_strategy'] in ['maP', 'day_losers']:
       return low_price
   elif row['trading_strategy'] in ['most_actives', 'day_gainers']:
       return high_price
   else:
        return 0
   

def calculate_floor_pct(row):
   from_stamp = row['order_transaction_date'].split('T')[0]
   time_stamp = datetime.strptime(row['order_transaction_date'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() - timedelta(hours=4).total_seconds()
   prices = polygon_call_stocks(row['underlying_symbol'], from_stamp, current_date, "1", "hour")
   trimmed_df = prices.loc[prices['t'] > time_stamp]
   high_price = trimmed_df['h'].max()
   low_price = trimmed_df['l'].min()
   if row['trading_strategy'] in ['maP', 'day_losers']:
       return low_price
   elif row['trading_strategy'] in ['most_actives', 'day_gainers']:
       return high_price
   else:
        return 0
   

def polygon_call(contract, from_stamp, to_stamp, multiplier, timespan):
    url = f"https://api.polygon.io/v2/aggs/ticker/O:{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"

    response = execute_polygon_call(url)
    res_df = pd.DataFrame(json.loads(response.text)['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    return res_df

def polygon_call_stocks(contract, from_stamp, to_stamp, multiplier, timespan):
    url = f"https://api.polygon.io/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"

    response = execute_polygon_call(url)
    res_df = pd.DataFrame(json.loads(response.text)['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    return res_df


def build_spread(chain_df, spread_length, cp):
    contract_list = []
    chain_df = chain_df.loc[chain_df['inTheMoney'] == False].reset_index(drop=True)
    if cp == "calls":
        chain_df = chain_df.iloc[:spread_length]
    if cp == "puts":
        chain_df = chain_df.iloc[-spread_length:]
    if len(chain_df) < spread_length:
        return contract_list
    for index, row in chain_df.iterrows():
        temp_object = {
            "contractSymbol": row['contractSymbol'],
            "strike": row['strike'],
            "lastPrice": row['lastPrice'],
            "bid": row['bid'],
            "ask": row['ask'],
            "quantity": 1
        }
        contract_list.append(temp_object)
    
    return contract_list

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