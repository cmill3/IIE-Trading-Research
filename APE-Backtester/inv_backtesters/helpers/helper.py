from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pytz

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

def get_day_diff(transaction_date, current_date):
    transaction_dt = datetime(transaction_date.year, transaction_date.month, transaction_date.day)
    current_dt = datetime(current_date.year, current_date.month, current_date.day)

    days_between = 0
    while transaction_dt < current_dt:
        transaction_dt += timedelta(days=1)
        if transaction_dt.weekday() < 5:
            days_between += 1
    return days_between

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


def convert_timestamp_est(timestamp):
    # Create a naive datetime object from the UNIX timestamp
    dt_naive = datetime.utcfromtimestamp(timestamp)
    # Convert the naive datetime object to a timezone-aware one (UTC)
    dt_utc = pytz.utc.localize(dt_naive)
    # Convert the UTC datetime to EST
    dt_est = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    
    return dt_est


if __name__ == "__main__":    
    res = get_day_diff(datetime(2023, 11, 17), datetime(2023, 11, 20))
    print(res)