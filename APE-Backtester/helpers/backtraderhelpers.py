from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def end_date(start_date, add_days):
    #Trading days only
    trading_days_to_add = add_days
    while trading_days_to_add > 0:
        start_date += timedelta(days=1)
        weekday = start_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    
    return start_date

def s3_data():
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    bucket_name = 'icarus-research-data'
    object_key = 'training_datasets/raw_1d_datasets/2023/01/02.csv'
    obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    rawdata = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(rawdata))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    return df

def polygon_data(x, from_date, to_date):
    multiplier = "30"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    optionsTicker = x
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)
    timespan = "minute"
    # from_stamp = standardize_dates(from_stamp)
    # to_stamp = standardize_dates(to_stamp)
    url = f"https://api.polygon.io/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.status_code)
    response_data = json.loads(response.text)
    res_option_df = pd.DataFrame(response_data['results'])
    res_option_df['t'] = res_option_df['t'].apply(lambda x: int(x/1000))
    res_option_df['date'] = res_option_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    res_option_df.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-BAcktester/APE-Backtester-Results/Testing_Research_Data_CSV_{x}_{from_date}.csv')
    return res_option_df

def set_data(x, from_date, to_date):
    data = bt.feeds.GenericCSVData(
        dataname= f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_RBLX_2023-01-02 14/00/00.csv',
        fromdate=from_date,
        todate=to_date,
        dtformat=('%Y-%m-%d %H:%M:%S'),
        datetime=9,
        open = 3,
        high = 5,
        low = 6,
        close = 4,
        volume =1, 
        openinterest=-1,
        reverse=False)
    return data
