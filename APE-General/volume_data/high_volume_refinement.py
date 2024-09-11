import pandas as pd
import numpy as np
import boto3
import requests
import ast
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import pytz
import concurrent.futures
import pandas_market_calendars as mcal
import os


nyse = mcal.get_calendar('NYSE')
holidays = nyse.regular_holidays
market_holidays = holidays.holidays()
KEY = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
s3 = boto3.client('s3')

def volume_coordinator(dates):
    failed_list = []
    for date in dates:
        try:
            path = create_s3_path(date)
            bucket = "icarus-research-data"
            df = pull_historical_s3_csv(bucket,path)
            filtered_df = volume_isolation(df)
            put_historical_s3(date, filtered_df)
        except Exception as e:
            print(e)
            print("Volume coordination failed for " + str(date))
            failed_list.append(date)
    failed_dates.append(failed_list)
            

def create_s3_path(date_str):
    year = date_str.split('-')[0]
    month = date_str.split('-')[1]
    day = date_str.split('-')[2]
    path = f'historical_volume_data/{year}/{month}/{day}/volume_report.csv'
    return path


def pull_historical_s3_csv(bucket, prefix):
    response = s3.get_object(Bucket = bucket, Key = prefix)
    df = pd.read_csv(response['Body'])
    return df

def volume_isolation(df):
    column_names = df.columns.to_list()
    column_names.pop(0)
    column_averages = df[column_names].mean(axis = 0).to_frame().reset_index()
    column_averages.columns = ['symbol','avg_volume']
    sorted_df = column_averages.sort_values(by = 'avg_volume', ascending = False)
    top50_df = sorted_df.iloc[:50].reset_index()
    top50_tickers = top50_df['symbol'].to_list()
    filtered_df = df[top50_tickers]
    filtered_df['datetime'] = df['datetime']

    return filtered_df

def put_historical_s3(date, df):
    date_key = date.replace('-', '/')
    s3.put_object(Body=df.to_csv(), Bucket="icarus-research-data", Key=f'top_50_historical_volume_data/{date_key}/volume_report.csv')
    return print("Top 50 upload for date " + date_key + " is successful.")


if __name__ == "__main__":
    cpu = os.cpu_count()
    start_date = datetime(2015,1,7)
    end_date = datetime(2024,4,4)
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y-%m-%d")
            date_list.append(date_str)
    failed_dates = []
    volume_coordinator(date_list)

    # run_process("2024-04-15")

    # with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
    #     # Submit the processing tasks to the ThreadPoolExecutor
    #     processed_weeks_futures = [executor.submit(volume_coordinator, date_str) for date_str in date_list]