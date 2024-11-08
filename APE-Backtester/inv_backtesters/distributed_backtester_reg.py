from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from helpers.constants import YEAR_CONFIG
import boto3
import helpers.distributed_backtester_helpers as back_tester
import warnings
import concurrent.futures
import os

warnings.filterwarnings("ignore")
bucket_name = 'icarus-research-data'  #s3 bucket name

holiday_weeks = ['2022-07-05','2022-11-21','2022-12-26','2023-11-20','2023-07-03']
idxbig = ["QQQ","SPY","NVDA","IWM","AAPL","TSLA","MSFT","AMD","AMZN","META","GOOGL"]

def build_backtest_data(file_name,strategies,config):
    dfs = []
    for strategy in strategies:
        try:
            name, prediction_horizon = strategy.split(":")
            print(f'/Users/charlesmiller/Documents/backtesting_data/{config["dataset"]}/{name}/{file_name}')
            data = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{config["dataset"]}/{name}/{file_name}')
            data['prediction_horizon'] = prediction_horizon
            data = data.loc[data['symbol'].isin(idxbig)]
            data['target_pct'] = (data['target'] - data['alert_price']) / data['alert_price']
            dfs.append(data)
        except FileNotFoundError:
            print(f"File {file_name} not found")
            continue
        
    backtest_data = pd.concat(dfs,ignore_index=True)
    return backtest_data


def backtest_orchestrator(file_names,strategies,config):
    all_trades = []
    for file_name in file_names:
        try:
            if config['holiday_weeks'] == False:
                if file_name in holiday_weeks:
                    continue

            trades = build_backtest_data(file_name,strategies,config)
            all_trades.append(trades)
        except Exception as e:
            continue

    # for trade in all_trades:
    #     merged_positions = back_tester.build_trade(trade,config)
    #     print(merged_positions)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=24) as executor:
        # Submit the processing tasks to the ThreadPoolExecutor
        processed_weeks_futures = [executor.submit(back_tester.build_trade,trades_df,config) for trades_df in all_trades]

    processed_weeks_results = [future.result() for future in processed_weeks_futures]
    print("process")

    merged_positions = []
    for week_results in processed_weeks_results:
        merged_positions.extend(week_results)
    
    print(len(merged_positions))
    print()
    return merged_positions

if __name__ == "__main__":
    s3 = boto3.client('s3')
    models_tested = []
    error_models = []
    nowstr = datetime.now().strftime("%Y%m%d")


    ## TREND STRATEGIES ONLY
    strategies = [
        "call_reg_2H:2H",
        # "put_reg_2H:2H",
    ]    

    backtest_configs = [
        {
            "spread_search": "0:6",
            "spread_length": 6,
            "model": "CDVOLREG",
            "volatility_threshold": 1,
            "vol_step": "1.1+1.2",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "dataset": "REGTEST",
            "holiday_weeks": False,
            "aa": 1,
            "minimum_vol_adjusted_target": 1,
        },
        {
            "spread_search": "0:6",
            "spread_length": 6,
            "model": "CDVOLREG",
            "volatility_threshold": 1,
            "vol_step": "1.1+1.2",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "dataset": "REGTEST",
            "holiday_weeks": False,
            "aa": 0,
            "minimum_vol_adjusted_target": 1,
        },
        {
            "spread_search": "0:6",
            "spread_length": 6,
            "model": "CDVOLREG",
            "volatility_threshold": 0.8,
            "vol_step": "1.1+1.2",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "dataset": "REGTEST",
            "holiday_weeks": False,
            "aa": 1,
            "minimum_vol_adjusted_target": 1,
        },
        # {
        #     "spread_search": "0:6",
        #     "spread_length": 6,
        #     "model": "CDVOLREG",
        #     "volatility_threshold": 1,
        #     "vol_step": "1.1+1.2",
        #     "user": "cm3",
        #     "threeD_vol": "return_vol_10D",
        #     "dataset": "REGTEST",
        #     "holiday_weeks": False,
        #     "aa": 0,
        #     "minimum_vol_adjusted_target": 1,
        # },
        ]
    
    modeling_type = "TST_REG"
    modeling_theme = "trend_threshold_reg"
    user = "cm3"
    date = datetime.now().strftime("%Y%m%d")

    for config in backtest_configs:
        model_name = f"2HREGf:sssl{config['spread_search']}-{config['spread_length']}_model{config['model']}_vt{config['volatility_threshold']}_vs{config['vol_step']}_aa{config['aa']}_mvat{config['minimum_vol_adjusted_target']}"
        try:
            file_names = os.listdir(f'/Users/charlesmiller/Documents/backtesting_data/{config["dataset"]}/{strategies[0].split(":")[0]}')
            print(file_names)
            positions_df = backtest_orchestrator(file_names,strategies, config)
            positions_df = pd.DataFrame.from_records(positions_df)
            res = s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{modeling_type}/{date}/{user}/{modeling_theme}/{model_name}/sim_results.csv')
        except Exception as e:
            print(f"Error running model: {e} for {model_name}")
            error_models.append(model_name)
            continue
        models_tested.append(model_name)

    print(f"Completed all models at {datetime.now()}!")
    print(models_tested)
    print("Errors:")
    print(error_models)