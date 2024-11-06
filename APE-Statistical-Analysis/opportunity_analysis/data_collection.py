import pandas as pd
import boto3 
import os
import json
import numpy as np
import requests
from datetime import datetime, timedelta
import pytz
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import concurrent.futures


KEY = "XpqF6xBLLrj6WALk4SS1UlkgphXmHQec"
s3 = boto3.client('s3')

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

def call_polygon(symbol,from_str,to_str,timespan,multiplier):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_str}/{to_str}?adjusted=false&sort=asc&limit=50000&apiKey={KEY}"

    try:
        response = execute_polygon_call(url)
        response_data = json.loads(response.text)
        results = response_data['results']
        results_df = pd.DataFrame(results)
        results_df['t'] = results_df['t'].apply(lambda x: int(x/1000))
        results_df['date'] = results_df['t'].apply(lambda x: convert_timestamp_est(x))
        results_df['dt_str'] = results_df['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        results_df['dt_str'] = results_df['dt_str'].apply(lambda x: x.rsplit('-',0)[0])
        results_df['date'] = results_df['dt_str'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        results_df['hour'] = results_df['date'].apply(lambda x: x.hour)
        results_df['day'] = results_df['date'].apply(lambda x: x.day)
        results_df['minute'] = results_df['date'].apply(lambda x: x.minute)
        results_df = results_df.loc[(results_df['hour'] >= 9) & (results_df['hour'] < 16)]
        results_df['symbol'] = symbol   
    except Exception as e:
        print(f"call polygon {e}")
        print(f"symbol {symbol}, dates {from_str} -  {to_str}, timespan {timespan}, multiplier {multiplier}")

    return results_df

def find_nearest_strike_price(strike_price, option_df, option_side):
    """
    Find the nearest strike price to the given strike price from a list of strike prices.
    
    Parameters:
    - strike_price: The strike price to find the nearest strike price to.
    - strike_prices: A list of strike prices to search.
    
    Returns:
    - The nearest strike price to the given strike price.
    """
    if option_side == 'call':
        option_df = option_df.loc[option_df['strike_price'] > strike_price]
        option_df = option_df.iloc[0:8]
    else:
        option_df = option_df.loc[option_df['strike_price'] < strike_price]
        option_df = option_df.iloc[-8:]
        

    return option_df

## given a list of strike dates and a strike date find the 3 nearest strike dates
def find_nearest_strike_dates(strike_date, strike_dates):
    """
    Find the 3 nearest strike dates to the given strike date from a list of strike dates.
    
    Parameters:
    - strike_date: The strike date to find the nearest strike dates to.
    - strike_dates: A list of strike dates to search.
    
    Returns:
    - The 3 nearest strike dates to the given strike date.
    """
    nearest_strike_dates = sorted(strike_dates, key=lambda x: abs((x - strike_date).days))[:3]
    return nearest_strike_dates

def parse_option_code(option_code):
    # Split the string into parts
    parts = option_code.split(':')[1]
    
    # Extract the date (assuming format YYMMDD)
    strike_date = parts[-15:-9]
    strike_date = datetime.strptime(strike_date, '%y%m%d')
    
    # Extract the strike price
    strike_price = float(parts[-8:]) / 1000  # Convert to dollars
    
    return strike_date, strike_price


def run_data_collection(date):
    print(f"running data collection for {date}")
    key_str = date.replace('-','/')
    dt = datetime.strptime(date,'%Y-%m-%d')
    end_dt = dt + timedelta(days=4)
    end_str = end_dt.strftime('%Y-%m-%d')
    info_dicts = []
    for symbol in ["QQQ","SPY","NVDA","AAPL","GOOG","GOOGL","MSFT","AMD","IWM","TSLA","META","NFLX","AMZN"]:
        try:
            options_df = s3.get_object(Bucket='icarus-research-data',Key=f'options_snapshot/{key_str}/{symbol}.csv')
            options_df = pd.read_csv(options_df['Body'])
            options_df[['strike_date','strike_price']] = options_df['symbol'].apply(lambda x: pd.Series(parse_option_code(x)))
            nearest_strike_dates = find_nearest_strike_dates(dt,options_df['strike_date'].unique())
            options_df = options_df.loc[options_df['strike_date'].isin(nearest_strike_dates)]
            for hour in range(10,16):
                underlying_agg_data = call_polygon(symbol,date,end_str,'hour',1)
                date_cutoff = datetime.strptime(f"{date} {hour}:00:00",'%Y-%m-%d %H:%M:%S')
                underlying_agg_data = underlying_agg_data.loc[underlying_agg_data['date'] > date_cutoff]
                open_price = underlying_agg_data['o'].iloc[0]
                price_data = underlying_agg_data.iloc[0:8]
                info_dicts.append({
                    'symbol': symbol,
                    'open_price': open_price,
                    'underlying_symbol': symbol,
                    'hour': hour,
                    'opens': price_data['o'].tolist(),
                    'closes': price_data['c'].tolist(),
                    'highs': price_data['h'].tolist(),
                    'lows': price_data['l'].tolist(),
                })
                for idx, strike in enumerate(nearest_strike_dates):
                    strike_df = options_df.loc[options_df['strike_date'] == strike]
                    for option_type in ['call','put']:
                        if option_type == 'call':
                            side_df = strike_df.loc[strike_df['option_type'] == 'call']
                        else:
                            side_df = strike_df.loc[strike_df['option_type'] == 'put']
                        filtered_df = find_nearest_strike_price(open_price,side_df,option_type)
                        for idx, row in filtered_df.iterrows():
                            try:
                                option_data = call_polygon(row['symbol'],date,end_str,'hour',1)
                                option_data = option_data.loc[option_data['date'] > date_cutoff]
                                option_data = option_data.iloc[:8]
                                info_dicts.append({
                                    'symbol': symbol,
                                    'contract_symbol': row['symbol'],
                                    'open_price': open_price,
                                    'underlying_symbol': symbol,
                                    'option_type': option_type,
                                    'hour': hour,
                                    'strike_distance': idx,
                                    'opens': option_data['o'].tolist(),
                                    'closes': option_data['c'].tolist(),
                                    'highs': option_data['h'].tolist(),
                                    'lows': option_data['l'].tolist(),
                                    'volume': option_data['v'].tolist(),
                                    })
                            except Exception as e:
                                print(f"error {e}")
                                print(f"symbol {symbol}, contract {row['symbol']}, date {date}, hour {hour}")
        except Exception as e:
            print(f"error {e}")
            print(f"symbol {symbol}, date {date}")

    info_df = pd.DataFrame(info_dicts)
    s3.put_object(Body=info_df.to_csv(), Bucket="icarus-research-data", Key=f'options_price_action_data/{key_str}.csv')
    print(f"finished data collection for {date}")
    return 
                    

                     


if __name__ == "__main__":
    start_date = datetime(2021,1,1)
    end_date = datetime(2024,9,1)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_list.append(temp_date.strftime('%Y-%m-%d'))
        
    # run_data_collection('2022-01-06')

    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(run_data_collection, date_str) for date_str in date_list]




