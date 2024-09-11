import pandas as pd
import numpy as np
import boto3
import logging
import pandas_market_calendars as mcal
from datetime import date, datetime, timedelta
from helpers.volume_helper import *

## filter out holidays using NYSE calendar
nyse = mcal.get_calendar('NYSE')
holidays = nyse.regular_holidays
market_holidays = holidays.holidays()
KEY = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
s3 = boto3.client('s3')
logger = logging.getLogger()

def run_process(event,context):
    build_SP500_vol_data()
    logger.info(f"Finished {date}")

def build_SP500_vol_data():
    bucket = 'icarus-research-data'
    key = 'sp500-constituents'
    df = pull_monthly_sp500_constituents_s3(key, bucket)
    date = datetime.today()
    if date.weekday() < 5:
            date_str = date.strftime("%Y-%m-%d")
    create_15min_df(df, date_str)
    volume_coordinator(date_str)

def create_15min_df(init_df, date):
    ##Perform 9-5
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    day = int(date.split('-')[2])
    start_date = datetime(year, month, day, 9, 0, 0)
    end_date = start_date + timedelta(hours=8)
    increment = 15
    period = 'minutes'
    times = date_range(start_date, end_date, increment, period)
    df = init_df.pivot(columns = 'symbols')
    ticker_list = init_df['symbols'].tolist()
    df = pd.DataFrame(times, columns=['datetime'])
    df.set_index('datetime', inplace=True)
    full_df, from_str = pull_volume_data(df, ticker_list, times, start_date, end_date)

        # for i in row['tickers'].split(','):
    # df.columns = ticker_list

    from_key = from_str.replace('-', '/')
    s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'historical_volume_data/{from_key}/volume_report.csv')
    print(f"Successful volume run for the date {from_str}")
    return "done"

def volume_coordinator(date):
    try:
        path = create_s3_path(date)
        bucket = "icarus-research-data"
        df = pull_s3_csv(bucket,path)
        filtered_df = volume_isolation(df)
        put_historical_s3(date, filtered_df)
    except Exception as e:
        logger.error(e)
        logger.error("Volume coordination failed for " + str(date))

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



if __name__ == '__main__':
    run_process(None, None)