from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import holidays
import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import helpers.portfolio_simulation as portfolio_sim
import warnings
import concurrent.futures
# from pandas._libs.mode_warnings import SettingWithCopyWarning


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


bucket_name = 'icarus-research-data'  #s3 bucket name
object_keybase = 'training_datasets/expanded_1d_datasets/' #s3 key not including date, date is added in pullcsv func

def build_backtest_data(file_name,strategies,config):
    full_purchases_list = []
    full_positions_list = []
    full_sales_list = []

    dfs = []
    for strategy in strategies:
        data = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{strategy}/{file_name}.csv')
        dfs.append(data)

    backtest_data = pd.concat(dfs,ignore_index=True)
    backtest_data = backtest_data[backtest_data['probabilities'] > config['probability']]

    ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
    positions_list = back_tester.simulate_trades_invalerts(backtest_data,config)
    full_positions_list.extend(positions_list)

    return positions_list

def run_trades_simulation(full_positions_list,start_date,end_date,config):
    full_date_list = helper.create_portfolio_date_list(start_date, end_date)
    if config['pos_limit']:
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio_poslimit(
            full_positions_list, full_date_list,portfolio_cash=config['portfolio_cash'], risk_unit=config['risk_unit'],put_adjustment=config['put_pct']
            )
    else:
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio(
            full_positions_list, full_date_list,portfolio_cash=config['portfolio_cash'], risk_unit=config['risk_unit'],put_adjustment=config['put_pct']
            )
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df

def backtest_orchestrator(start_date,end_date,file_names,strategies,local_data,config):

    if not local_data:
        with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
            # Submit the processing tasks to the ThreadPoolExecutor
            processed_weeks_futures = [executor.submit(build_backtest_data,file_name,strategies,config) for file_name in file_names]

        # Step 4: Retrieve the results from the futures
        processed_weeks_results = [future.result() for future in processed_weeks_futures]

        merged_positions = []
        for week_results in processed_weeks_results:
            merged_positions.extend(week_results)

        # merged_df = pd.DataFrame.from_dict(merged_positions)
        # merged_df.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/merged_positions.csv', index=False)
    else:
        merged_positions = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/merged_positions.csv')
        merged_positions = merged_positions.to_dict('records')

    portfolio_df, positions_df = run_trades_simulation(merged_positions, start_date, end_date, config)
    return portfolio_df, positions_df

if __name__ == "__main__":
    s3 = boto3.client('s3')
    # '2022-01-03', '2022-01-10', '2022-01-17', '2022-01-24', '2022-01-31', '2022-02-07', '2022-02-14', '2022-02-21', 
    #      '2022-02-28', '2022-03-07', '2022-03-14', '2022-03-21', '2022-03-28', '2022-04-04', '2022-04-11', '2022-04-18', 
    #      '2022-04-25', '2022-05-02', '2022-05-09', '2022-05-16', '2022-05-23', '2022-05-30', '2022-06-06', '2022-06-13', 
    #      '2022-06-20', '2022-06-27', '2022-07-04', '2022-07-11', '2022-07-18', '2022-07-25', '2022-08-01', '2022-08-08', 
    #      '2022-08-15', '2022-08-22', '2022-08-29', '2022-09-05', '2022-09-12', '2022-09-19', '2022-09-26', '2022-10-03', 
    #      '2022-10-10', '2022-10-17', '2022-10-24', '2022-10-31', '2022-11-07', '2022-11-14', '2022-11-21', '2022-11-28', 
    #      '2022-12-05', '2022-12-12', '2022-12-19', '2022-12-26', 2023-10-16', '2023-10-23', '2023-10-30', 
        #  '2023-11-06'
    # test_files = [ 
    #     '2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
    #      '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
    #      '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
    #      '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26', '2023-07-03', '2023-07-10', 
    #      '2023-07-17', '2023-07-24', '2023-07-31', '2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28', '2023-09-04', 
    #      '2023-09-11', '2023-09-18', '2023-09-25', '2023-10-02']
    file_names = [
         '2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
         '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
         '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
         '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26', '2023-07-03', '2023-07-10', 
         '2023-07-17', '2023-07-24', '2023-07-31', '2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28', '2023-09-04', 
         '2023-09-11', '2023-09-18', '2023-09-25', '2023-10-02', '2023-10-09'
         ]

    test_files =  ['2023-08-14', '2023-08-21', '2023-08-28', '2023-09-04', 
    '2023-09-11', '2023-09-18', '2023-09-25', '2023-10-02']
    test_files2 = ['2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
         '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20']
    test_files3 = ['2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
         '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24']
    test_files4 = ['2023-05-01', '2023-05-08', '2023-05-15', 
         '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19']
    
    time_periods = [test_files,test_files2,test_files3,test_files4]
    models_tested = []
    for time in time_periods:
        print(f"Starting {time} at {datetime.now()}")
        start_dt = time[0]
        end_date = time[-1]

        start_date = start_dt.replace("-","/")
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=7)
        end_date = end_dt.strftime("%Y/%m/%d")
        start_str = start_date.split("/")[1] + start_date.split("/")[2]
        end_str = end_date.split("/")[1] + end_date.split("/")[2]
        strategies = ["BFC","BFC_1D","BFP","BFP_1D"]

        backtest_configs = [
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .01,
                "vc": False,
                "vc_level":"nvc",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 1,
                "aa": 1,
                "risk_unit": .01,
                "vc": False,
                "vc_level":"nvc",
                "portfolio_cash": 200000,
                "risk_adjustment": .005,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc",
                "vc_level":"250$.005",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .01,
                "vc": "vc",
                "vc_level":"150$.005",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .01,
                "vc": "vc",
                "vc_level":"250$.003",
                "portfolio_cash": 200000,
                "risk_adjustment": .005,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .01,
                "vc": "vc",
                "vc_level":"150$.003",
                "portfolio_cash": 200000,
                "risk_adjustment": .005,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc",
                "vc_level":"250$.005",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": False
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc",
                "vc_level":"150$.005",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": False
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc",
                "vc_level":"250$.003",
                "portfolio_cash": 200000,
                "risk_adjustment": .005,
                "probability": 0.5,
                "pos_limit": False
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc",
                "vc_level":"150$.003",
                "portfolio_cash": 200000,
                "risk_adjustment": .005,
                "probability": 0.5,
                "pos_limit": False
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .01,
                "vc": "vc2",
                "vc_level":"400$.005",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .01,
                "vc": "vc2",
                "vc_level":"300$.005",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": True
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc2",
                "vc_level":"400$.003",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": False
            },
            {
                "put_pct": 1, 
                "spread_adjustment": 0,
                "aa": 1,
                "risk_unit": .002,
                "vc": "vc2",
                "vc_level":"300$.003",
                "portfolio_cash": 200000,
                "risk_adjustment": .003,
                "probability": 0.5,
                "pos_limit": False
            }
        ]

        for config in backtest_configs:
            trading_strat = f"modelsv2_noposlimit_{config['spread_adjustment']}out_{config['put_pct']}put_noresize_risk{config['risk_adjustment']}_AA{config['aa']}_{config['vc_level']}_prob{config['probability']}"
            print(f"Starting {trading_strat} at {datetime.now()}")
            portfolio_df, positions_df = backtest_orchestrator(start_date, end_date,file_names=time,strategies=strategies,local_data=False, config=config) 
            s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
            s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
            print(f"Done with {trading_strat} at {datetime.now()}!")
            models_tested.append(trading_strat)

    print(f"Completed all models at {datetime.now()}!")
    print(models_tested)


