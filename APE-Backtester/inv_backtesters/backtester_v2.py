from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import holidays
from xone import calendar
import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import warnings
# from pandas._libs.mode_warnings import SettingWithCopyWarning


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


bucket_name = 'icarus-research-data'  #s3 bucket name
object_keybase = 'training_datasets/expanded_1d_datasets/' #s3 key not including date, date is added in pullcsv func

def run_backtest(start_date, end_date):
    btdays = helper.tradingdaterange(start_date, end_date)
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
        # data, datetime_list = back_tester.pull_data(s3_link)
        # #     ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
        # purchases_list, sales_list, order_results_list, positions_list, = back_tester.simulate_trades(data, datetime_list, "starting_value", "null", s3_link)
        # full_purchases_list.extend(purchases_list)
        # full_positions_list.extend(positions_list)
        # full_sales_list.extend(sales_list)
        try:
            data, datetime_list = back_tester.pull_data(s3_link)
            #     ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
            purchases_list, sales_list, order_results_list, positions_list, = back_tester.simulate_trades(data, datetime_list, "starting_value", "null", s3_link)
            full_purchases_list.extend(purchases_list)
            full_positions_list.extend(positions_list)
            full_sales_list.extend(sales_list)
        except Exception as e:
            print(e)
            print(item)
            continue

    # results_df = backtrader.build_results_df(full_purchases_list, full_sales_list, datetime_list)
    full_date_list = helper.create_portfolio_date_list(start_date.replace("/","-")+" 13:00:00", end_date.replace("/","-")+" 20:00:00")
    portfolio_df, passed_trades_df, positions_taken, positions_dict = helper.simulate_portfolio(full_positions_list, full_date_list,100000)
    # positions_df, positions_dict = helper.build_positions_df(full_positions_list, positions_dict)
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df


def run_backtest_inv_alerts(file_names):
    full_purchases_list = []
    full_positions_list = []
    full_sales_list = []

    for file in file_names:

        data, datetime_list = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts_contracts", file_name = f"{file}.csv")
        ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
        purchases_list, sales_list, order_results_list, positions_list, = back_tester.simulate_trades_invalerts(data)
        full_purchases_list.extend(purchases_list)
        full_positions_list.extend(positions_list)
        full_sales_list.extend(sales_list)
        # try:
        #     data, datetime_list = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/", file_name = file)
        #     ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
        #     purchases_list, sales_list, order_results_list, positions_list, = back_tester.simulate_trades_invalerts(data)
        #     full_purchases_list.extend(purchases_list)
        #     full_positions_list.extend(positions_list)
        #     full_sales_list.extend(sales_list)
        # except Exception as e:
        #     print(e)
        #     print(file)
        #     continue

    # results_df = backtrader.build_results_df(full_purchases_list, full_sales_list, datetime_list)
    full_date_list = helper.create_portfolio_date_list(start_date.replace("/","-")+" 13:00:00", end_date.replace("/","-")+" 20:00:00")
    portfolio_df, passed_trades_df, positions_taken, positions_dict = helper.simulate_portfolio(full_positions_list, full_date_list,100000)
    # positions_df, positions_dict = helper.build_positions_df(full_positions_list, positions_dict)
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df

if __name__ == "__main__":
    start_date = '2023/01/02'
    end_date = '2023/01/13'
    start_str = start_date.split("/")[1] + start_date.split("/")[2]
    end_str = end_date.split("/")[1] + end_date.split("/")[2]
    trading_strat = "inv_test"
    # portfolio_df, positions_df = run_backtest(start_date, end_date)

    file_names = ["2023-01-02","2023-01-09","2023-01-16","2023-01-23","2023-01-30","2023-02-06",
                  "2023-02-13","2023-02-20","2023-02-27","2023-03-06"
                  ,"2023-03-13","2023-03-20","2023-03-27","2023-04-03","2023-04-10","2023-04-17",
                  "2023-04-24","2023-05-01","2023-05-08","2023-05-15","2023-05-22","2023-05-29","2023-06-05"]
    portfolio_df, positions_df = run_backtest_inv_alerts(file_names[0:2])    

    
    portfolio_df.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/Icarus/APE-Research/APE-Backtester/data/{trading_strat}/2023-01-02-2023-06-05/portfolio_report1001X.csv')
    positions_df.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/Icarus/APE-Research/APE-Backtester/data/{trading_strat}/2023-01-02-2023-06-05/positions_report1001X.csv')
    print("Done!")