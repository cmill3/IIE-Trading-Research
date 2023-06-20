from datetime import datetime, timedelta
import requests
import pandas as pd
import json


def get_business_days(transaction_date, current_date):
    """
    Returns the number of business days (excluding weekends) between two dates. For now we
    aren't considering market holidays because that is how the training data was generated.
    """
    
    transaction_dt = datetime.strptime(transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    # We aren't inclusive of the transaction date
    days = (current_date - transaction_dt).days 

    complete_weeks = days // 7
    remaining_days = days % 7

    # Calculate the number of weekend days in the complete weeks
    weekends = complete_weeks * 2

    # Adjust for the remaining days
    if remaining_days:
        start_weekday = transaction_dt.weekday()
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
    payload={}
    headers = {}
    url = f"https://api.polygon.io/v2/aggs/ticker/O:{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"

    response = requests.request("GET", url, headers=headers, data=payload)
    res_df = pd.DataFrame(json.loads(response.text)['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    return res_df

def polygon_call_stocks(contract, from_stamp, to_stamp, multiplier, timespan):
    payload={}
    headers = {}
    url = f"https://api.polygon.io/v2/aggs/ticker/{contract}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"

    response = requests.request("GET", url, headers=headers, data=payload)
    res_df = pd.DataFrame(json.loads(response.text)['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    return res_df
