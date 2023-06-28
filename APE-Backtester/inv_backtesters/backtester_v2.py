from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import holidays
from xone import calendar
import boto3
import helpers.backtest_functions as backtester
import helpers.backtrader_helper as backtrader
import warnings
# from pandas._libs.mode_warnings import SettingWithCopyWarning


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


bucket_name = 'icarus-research-data'  #s3 bucket name
object_keybase = 'training_datasets/expanded_1d_datasets/' #s3 key not including date, date is added in pullcsv func

def run_backtest(startdate, enddate):
    btdays = backtrader.tradingdaterange(startdate, enddate)
    object_keydates = []

    for i in btdays:
        object_date = i + ".csv"
        object_keydates.append(object_date)
        #Need to reformat backtestfunctions to accept this info for the boto3 and polygon
    
    full_purchases_list = []
    full_positions_list = []
    full_sales_list = []

    for i, item in enumerate(object_keydates):
        s3_link = {
        'bucketname': 'icarus-research-data',
        'objectkey': f'training_datasets/expanded_1d_datasets/{item}'
        }
        # starting_value, commission_cost, raw_data, data, datetime_list, results = backtester.kickoff(s3_link)
        # purchases_list, sales_list, order_results_list, positions_list, = backtester.simulate_trades(data, datetime_list, starting_value, commission_cost, s3_link)
        # full_purchases_list.extend(purchases_list)
        # full_positions_list.extend(positions_list)
        # full_sales_list.extend(sales_list)
        try:
            starting_value, commission_cost, raw_data, data, datetime_list, results = backtester.kickoff(s3_link)
            ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
            purchases_list, sales_list, order_results_list, positions_list, = backtester.simulate_trades(data, datetime_list, starting_value, commission_cost, s3_link)
            full_purchases_list.extend(purchases_list)
            full_positions_list.extend(positions_list)
            full_sales_list.extend(sales_list)
        except Exception as e:
            print(e)
            print(item)
            continue

    results_df = backtrader.build_results_df(full_purchases_list, full_sales_list, datetime_list)
    positions_df = backtrader.build_positions_df(full_positions_list)
    
    return results_df, positions_df

if __name__ == "__main__":
    start_date = '2023/05/09'
    end_date = '2023/06/16'
    start_str = "0509"
    end_str = "0617"
    trading_strat = "3spread_sizing"

    trades_df, positions_df = run_backtest(start_date, end_date)
    trades_df.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/Icarus/APE-Research/APE-Backtester/data/{trading_strat}/{start_str}-{end_str}/portfolio_report.csv')
    positions_df.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/Icarus/APE-Research/APE-Backtester/data/{trading_strat}/{start_str}-{end_str}/positions_report.csv')
    print("Done!")