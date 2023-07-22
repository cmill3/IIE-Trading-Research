from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import holidays
import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import warnings
import concurrent.futures
# from pandas._libs.mode_warnings import SettingWithCopyWarning


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


bucket_name = 'icarus-research-data'  #s3 bucket name
object_keybase = 'training_datasets/expanded_1d_datasets/' #s3 key not including date, date is added in pullcsv func

def build_backtest_data(file_name):
    full_purchases_list = []
    full_positions_list = []
    full_sales_list = []


    data, datetime_list = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/priceFeaturesnoPCR", file_name = f"{file_name}.csv")
    ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
    purchases_list, sales_list, order_results_list, positions_list, = back_tester.simulate_trades_invalerts(data)
    full_purchases_list.extend(purchases_list)
    full_positions_list.extend(positions_list)
    full_sales_list.extend(sales_list)

    return positions_list
    # return full_purchases_list, full_positions_list, full_sales_list, datetime_list

def run_trades_simulation(full_positions_list,portfolio_cash, start_date, end_date, risk_unit):
    # results_df = backtrader.build_results_df(full_purchases_list, full_sales_list, datetime_list)
    full_date_list = helper.create_portfolio_date_list(start_date.replace("/","-")+" 13:00:00", end_date.replace("/","-")+" 20:00:00")
    portfolio_df, passed_trades_df, positions_taken, positions_dict = helper.simulate_portfolio(full_positions_list, full_date_list,portfolio_cash=portfolio_cash, risk_unit=risk_unit)
    # positions_df, positions_dict = helper.build_positions_df(full_positions_list, positions_dict)
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df

def backtest_orchestrator(start_date, end_date, portfolio_cash, risk_unit,file_names):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(build_backtest_data, file_name) for file_name in file_names]

    # Step 4: Retrieve the results from the futures
    processed_weeks_results = [future.result() for future in processed_weeks_futures]

    merged_positions = []
    for week_results in processed_weeks_results:
        merged_positions.extend(week_results)

    portfolio_df, positions_df = run_trades_simulation(merged_positions,portfolio_cash, start_date, end_date, risk_unit)
    return portfolio_df, positions_df

if __name__ == "__main__":
    s3 = boto3.client('s3')
    start_date = '2023/05/08'
    end_date = '2023/06/12'
    start_str = start_date.split("/")[1] + start_date.split("/")[2]
    end_str = end_date.split("/")[1] + end_date.split("/")[2]
    trading_strat = "inv_basic_PF"
    portfolio_cash = 250000
    risk_unit =.01
    cash_risk = f"{portfolio_cash}_{risk_unit}"
    # portfolio_df, positions_df = run_backtest(start_date, end_date)

    file_names = ["2023-01-02","2023-01-09","2023-01-16","2023-01-23","2023-01-30","2023-02-06",
                  "2023-02-13","2023-02-20","2023-02-27","2023-03-06"
                  ,"2023-03-13","2023-03-20","2023-03-27","2023-04-03","2023-04-10","2023-04-17",
                  "2023-04-24","2023-05-01","2023-05-08","2023-05-15","2023-05-22","2023-05-29","2023-06-05"]
    portfolio_df, positions_df = backtest_orchestrator(start_date, end_date,portfolio_cash=portfolio_cash,risk_unit=risk_unit,file_names=file_names[-5:])    

    
    port_csv = portfolio_df.to_csv()
    pos_csv = positions_df.to_csv()
    s3.put_object(Body=port_csv, Bucket="icarus-research-data", Key=f'backtesting_reports/{trading_strat}/{start_str}-{end_str}/{cash_risk}/portfolio_report.csv')
    s3.put_object(Body=pos_csv, Bucket="icarus-research-data", Key=f'backtesting_reports/{trading_strat}/{start_str}-{end_str}/{cash_risk}/positions_report.csv')
    print("Done!")