import boto3
import helpers.backtest_functions as back_tester
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import pandas_market_calendars as mcal
import numpy as np
from helpers.constants import ONED_STRATEGIES, THREED_STRATEGIES, YEAR_CONFIG
import ast

s3 = boto3.client('s3')
nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

def pull_potential_trades(run_info):
    dfs = []
    for strategy in run_info['strategies']:
        for date in run_info['dates']:
            for hour in ["10","11","12","13","14","15"]:
                key = f"invalerts_potential_trades/PROD_VAL/{strategy}/{date}/{hour}.csv"
                try:
                    contracts = s3.get_object(Bucket="inv-alerts-trading-data", Key=key)
                except Exception as e:
                    print(f"Error pulling data: {e} for {strategy}")
                    return []
                contracts = pd.read_csv(contracts['Body'])
                dfs.append(contracts)
    contracts = pd.concat(dfs,ignore_index=True)
    contracts['probabilities'] = contracts['classifier_prediction'].astype(int)
    trade_data = contracts[contracts['classifier_prediction'] > 0.5]
    trade_data['trade_details1wk'] = trade_data['trade_details1wk'].apply(lambda x: ast.literal_eval(x))
    trade_data['num_contracts'] = trade_data['trade_details1wk'].apply(lambda x: len(x))
    trade_data = trade_data.loc[trade_data['num_contracts'] > 0]
    print(trade_data)


    

        
def pull_contract_data(row):
    if row['symbol'] in ['SPY','QQQ','IWM']:
        date = row['date'].split(" ")[0]
        file_date = create_index_date(row['date'])
        year, month, day = file_date.strftime('%Y-%m-%d').split("-")
    elif row['symbol'] in ['NVDA','GOOG','GOOGL','AMZN','TSLA']:
        date = row['date'].split(" ")[0]
        year, month, day = date.split("-")
        if year in ['2021','2022']:
            dt = datetime(int(year),int(month),int(day))
            weekday = dt.weekday()
            monday_dt = dt - timedelta(days=weekday)
            monday_str = monday_dt.strftime('%Y-%m-%d')
            year, month, day = monday_str.split("-")
    else:
        date = row['date'].split(" ")[0]
        year, month, day = date.split("-")
    key = f"options_snapshot/{year}/{month}/{day}/{row['symbol']}.csv"
    try:
        contracts = s3.get_object(Bucket="icarus-research-data", Key=key)
    except Exception as e:
        print(f"Error pulling data: {e} for {row['symbol']}")
        return []
    contracts = pd.read_csv(contracts['Body'])
    try:
        contracts['date'] = contracts['symbol'].apply(lambda x: x[-15:-9])
        contracts['side'] = contracts['symbol'].apply(lambda x: x[-9])
        contracts['year'] = contracts['date'].apply(lambda x: f"20{x[:2]}")
        contracts['month'] = contracts['date'].apply(lambda x: x[2:4])
        contracts['day'] = contracts['date'].apply(lambda x: x[4:])
        contracts['date'] = contracts['year'] + "-" + contracts['month'] + "-" + contracts['day']
        expiry_dates = generate_expiry_dates(date,row['symbol'],row['strategy'])
        filtered_contracts = contracts[contracts['date'].isin(expiry_dates)]
        filtered_contracts = filtered_contracts[filtered_contracts['side'] == row['side']]
        contracts_list = filtered_contracts['symbol'].tolist()
    except Exception as e:
        print(f"Error: {e} for {row['symbol']}")
        print(row)
        print(row['symbol'])
        print(contracts)
        print()
        return []
    return contracts_list


def generate_expiry_dates(date_str,symbol,strategy):
    if symbol in ['SPY','QQQ','IWM']:
        if strategy in ONED_STRATEGIES:
            day_of = add_weekdays(date_str,1,symbol)
            next_day = add_weekdays(date_str,2,symbol)
            return [day_of.strftime('%Y-%m-%d'),next_day.strftime('%Y-%m-%d')]
        elif strategy in THREED_STRATEGIES:
            day_of = add_weekdays(date_str,3,symbol)
            next_day = add_weekdays(date_str,4,symbol)
            return [day_of.strftime('%Y-%m-%d'),next_day.strftime('%Y-%m-%d')]
    else: 
        input_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Find the weekday of the input date (Monday is 0 and Sunday is 6)
        weekday = input_date.weekday()


    if weekday == 4:
        closest_friday = input_date + timedelta(days=7)
        following_friday = input_date + timedelta(days=14)
        return [closest_friday.strftime('%Y-%m-%d'), following_friday.strftime('%Y-%m-%d')]

    # Calculate days until the next Friday
    days_until_closest_friday = (4 - weekday) % 7
    days_until_following_friday = days_until_closest_friday + 7
    closest_friday = input_date + timedelta(days=days_until_closest_friday)
    following_friday = input_date + timedelta(days=days_until_following_friday)

    return [closest_friday.strftime('%Y-%m-%d'), following_friday.strftime('%Y-%m-%d')]

def create_index_date(date):
    str = date.split(" ")[0]
    dt = datetime.strptime(str, '%Y-%m-%d')
    wk_day = dt.weekday()
    monday = dt - timedelta(days=wk_day)
    monday_np = np.datetime64(monday)
    
    if monday_np in holidays_multiyear:
        monday = monday + timedelta(days=1)
    return monday

def generate_expiry_dates_row(row):
    date_str = row['date'].split(" ")[0]
    if row['symbol'] in ['SPY','QQQ','IWM']:
        if row['strategy'] in ONED_STRATEGIES:
            day_of = add_weekdays(date_str,1,row['symbol'])
            next_day = add_weekdays(date_str,2,row['symbol'])
            return [day_of.strftime('%y%m%d'),next_day.strftime('%y%m%d')]
        elif row['strategy'] in THREED_STRATEGIES:
            day_of = add_weekdays(date_str,3,row['symbol'])  
            next_day = add_weekdays(date_str,4,row['symbol'])
            return [day_of.strftime('%y%m%d'),next_day.strftime('%y%m%d')]
    else: 
        input_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Find the weekday of the input date (Monday is 0 and Sunday is 6)
        weekday = input_date.weekday()

    if weekday == 4:
        closest_friday = input_date + timedelta(days=7)
        following_friday = input_date + timedelta(days=14)

        return [closest_friday.strftime('%y%m%d'), following_friday.strftime('%y%m%d')]

    # Calculate days until the next Friday
    days_until_closest_friday = (4 - weekday) % 7
    days_until_following_friday = days_until_closest_friday + 7
    closest_friday = input_date + timedelta(days=days_until_closest_friday)
    following_friday = input_date + timedelta(days=days_until_following_friday)


    return [closest_friday.strftime('%y%m%d'), following_friday.strftime('%y%m%d')]

def add_weekdays(date,days,symbol):
    if type(date) == str:
        date = datetime.strptime(date, '%Y-%m-%d')
    year = date.year
    month = date.month
    # date = datetime.strptime(date, '%Y-%m-%d')
    while days > 0:
        date += timedelta(days=1)
        if date.weekday() < 5:
            days -= 1

    ## works fully for now, will need to fix if we expand to taking calendar spreads as well
    if symbol == "IWM" or year == 2021 or (year == 2022 and month < 6):
        if date.weekday() in [1,3]:
            date += timedelta(days=1)
    return date

if __name__ == "__main__":
    run_info = {
        "strategies": ['CDBFC','CDBFP','CDBFC_1D','CDBFP_1D'],
        "dates": ["2024/04/29"],
    }
    pull_potential_trades(run_info)

