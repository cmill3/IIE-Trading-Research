from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json
import ast


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def s3_data():
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    bucket_name = 'icarus-research-data'
    object_key = 'training_datasets/expanded_1d_datasets/2023/04/17.csv'
    obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    rawdata = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(rawdata))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    df['contracts'] = df['contracts'].apply(lambda x: ast.literal_eval(x))
    df['contracts_available'] = df['contracts'].apply(lambda x: len(x)>=12)
    return df

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

def polygon_optiondata(x, from_date, to_date):
    #This is for option data
    multiplier = "15"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    optionsTicker = x
    print(optionsTicker)
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

rawdata = s3_data()
start_date = datetime.strptime(rawdata['date'].values[42], '%Y-%m-%d %H:%M:%S')
end_date = end_date(start_date, 3)


def build_table():
    days = pd.date_range(start_date, end_date, freq='15min', name = 'Time')
    results = pd.DataFrame(days, columns= ['datetime'])
    # index = pd.Index(days)
    # results = results.set_index(index)
    # results.drop(columns=['Days'])
    return days,results

def convertepoch(time):
    # closetime = int(epochtime) / 1000000000
    # closetime_esttogmt = closetime + 14400
    closedate = datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
    finaldate = datetime.strptime(closedate, '%Y-%m-%d %H:%M:%S')
    return finaldate

dtdt = convertepoch(1683647027)
print(dtdt)


# days, df2 = build_table()
# df = s3_data()

# print(days)
# print(df2)

# print(df['date'][0])