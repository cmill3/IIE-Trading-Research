import boto3
import helpers.backtest_functions as back_tester
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import pandas_market_calendars as mcal
import numpy as np
from helpers.constants import *
from helpers import backtrader_helper

s3 = boto3.client('s3')
nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

def list_files_in_bucket(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = []
    for content in response['Contents']:
        if content['Key'].split("/")[-1] == 'model_config.csv':
            continue
        files.append(content['Key'])    
    return files

def add_contract_data_to_local(strategy_info,data_type,key):
        week = key.split("/")[-1].split(".")[0]
        # try:
        data, _ = pull_data_invalerts(bucket_name=strategy_info['bucket'], file_name=key, 
                                                strategy_name=strategy_info['strategy'],time_span=strategy_info['time_span'])
            # data.drop(columns=['Unnamed: 0.2','Unnamed: 0.1','Unnamed: 0'],inplace=True)
        data['side'] = strategy_info['side']
        data['contracts']= data.apply(lambda x: pull_contract_data(x),axis=1)
        data['expiries'] = data.apply(lambda x: generate_expiry_dates_row(x),axis=1)
        week_str = week.split(" ")[0]
        print(f"/Users/charlesmiller/Documents/backtesting_data/{data_type}/{strategy_info['strategy']}/{week_str}.csv")
        data.to_csv(f"/Users/charlesmiller/Documents/backtesting_data/{data_type}/{strategy_info['strategy']}/{week_str}.csv", index=False)
        print(f"Finished {strategy_info['strategy']} for {week}")
        # except Exception as e:
        #     print(f"Error: {e} for {strategy}")
    

def pull_data_invalerts(bucket_name, file_name, strategy_name, time_span):
    try:
        print(f"{file_name}")
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = pd.read_csv(obj.get("Body"))
        data['strategy'] = strategy_name
    except Exception as e:
        print(f"no file for {strategy_name} with {e}")
        return [], []
    start_time = datetime.strptime(data['date'].values[0], '%Y-%m-%d')
    end_date = backtrader_helper.create_end_date_local_data(data['date'].values[-1], time_span)
    datetime_list, datetime_index, results = backtrader_helper.create_datetime_index(start_time, end_date)
    return data, datetime_list
        
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
        if strategy in TREND_STRATEGIES_2H:
            day_of = add_weekdays(date_str,1,symbol)
            next_day = add_weekdays(date_str,2,symbol)
            return [day_of.strftime('%Y-%m-%d'),next_day.strftime('%Y-%m-%d')]
        elif strategy in TREND_STRATEGIES_3D:
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
        if row['strategy'] in TREND_STRATEGIES_2H:
            day_of = add_weekdays(date_str,1,row['symbol'])
            next_day = add_weekdays(date_str,2,row['symbol'])
            return [day_of.strftime('%y%m%d'),next_day.strftime('%y%m%d')]
        elif row['strategy'] in TREND_STRATEGIES_3D:
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
        strategy_info = [
            {
                "file_path": 'PTST_weekly_predictions/idxbig_puts_2H_241104',
                "bucket": "icarus-research-data",
                "time_span": 2,
                "side": "P",
                "strategy": "put_reg_2H",
            },
        ]
        data_type = 'REGTEST'

        
        # add_contract_data_to_local(file_names,strategy_info['GAIN'],"GAIN",'cls')
        for strategy in strategy_info:
            file_names = list_files_in_bucket(strategy['bucket'],strategy['file_path'])
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                # Submit the processing tasks to the ThreadPoolExecutor
                processed_weeks_futures = [executor.submit(add_contract_data_to_local,strategy,data_type,file_name) for file_name in file_names]
            # add_contract_data_to_local(strategy,data_type,file_names[0])

