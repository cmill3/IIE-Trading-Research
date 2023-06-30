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
            ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
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

if __name__ == "__main__":
    start_date = '2023/05/09'
    end_date = '2023/06/23'
    start_str = start_date.split("/")[1] + start_date.split("/")[2]
    end_str = end_date.split("/")[1] + end_date.split("/")[2]
    trading_strat = "portfolio_scaled_all_alerts"

    # weeks = [["2023/05/09", "2023/05/12"],["2023/05/15", "2023/05/19"],["2023/05/22", "2023/05/26"],["2023/05/29", "2023/06/02"],
    #          ["2023/06/05", "2023/06/09"],["2023/06/12", "2023/06/16"]]
    
    # backtest_results = {}
    # portfolio_dict = {}
    # positions_dict = {}
    # positions_taken = []
    # for week in weeks:
    #     start_date = week[0]
    #     end_date = week[1]
    #     start_str = start_date.split("/")[1] + start_date.split("/")[2]
    #     end_str = start_date.split("/")[1] + start_date.split("/")[2]
    #     portfolio_df, positions_df, positions_taken, portfolio_dict, positions_dict = run_backtest(start_date, end_date, portfolio_dict, positions_dict)
    #     backtest_results[f"{start_str}-{end_str}"] = {"portfolio_df": portfolio_df, "positions_df": positions_df}
    #     positions_taken.extend(positions_taken)

    portfolio_df, positions_df = run_backtest(start_date, end_date)
    portfolio_df.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/Icarus/APE-Research/APE-Backtester/data/{trading_strat}/{start_str}-{end_str}/portfolio_reportXX.csv')
    positions_df.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/Icarus/APE-Research/APE-Backtester/data/{trading_strat}/{start_str}-{end_str}/positions_reportXX.csv')
    print("Done!")