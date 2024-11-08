from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from helpers.constants import YEAR_CONFIG
import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import helpers.portfolio_simulation as portfolio_sim
import warnings
import concurrent.futures
import os


warnings.filterwarnings("ignore")
bucket_name = 'icarus-research-data'  #s3 bucket name

def build_backtest_data(file_name,strategies,config):
    full_purchases_list = []
    full_positions_list = []
    full_sales_list = []

    dfs = []
    for strategy in strategies:
        name, prediction_horizon = strategy.split(":")
        data = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{config["dataset"]}/{name}/{file_name}.csv')
        data['prediction_horizon'] = prediction_horizon
        dfs.append(data)
    
    backtest_data = pd.concat(dfs,ignore_index=True)
    # backtest_data = backtest_data[backtest_data['probabilities'] > config['probability']]
    # if config['model_type'] == "reg":
    #     predictions = helper.configure_regression_predictions(backtest_data,config)
    #     filtered_by_date = helper.configure_trade_data(predictions,config)
    # elif config['model_type'] == "cls":
    predictions = backtest_data.loc[backtest_data['predictions'] == 1]
    filtered_by_date = helper.configure_trade_data(predictions,config)
    
    ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
    # positions_list = back_tester.simulate_trades_invalerts(filtered_by_date,config)
    # full_positions_list.extend(positions_list)

    return filtered_by_date

def run_trades_simulation(full_positions_list,start_date,end_date,config,period_cash):
    full_date_list = helper.create_portfolio_date_list(start_date, end_date,config)
    if config['scale'] == "DS":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio_DS(
        full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
        config=config,results_dict_func=helper.extract_results_dict
        )
    elif config['scale'] == "DC":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio_daily_rebalance(
            full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
            config=config,results_dict_func=helper.extract_results_dict
            )
    elif config['scale'] == "FIX":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio_FIXED(
            full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
            config=config,results_dict_func=helper.extract_results_dict
            )
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df

def backtest_orchestrator(start_date,end_date,file_name,strategies,local_data,config,period_cash):
    merged_positions = build_backtest_data(file_name,strategies,config)
    merged_df = pd.DataFrame.from_dict(merged_positions)
    portfolio_df, positions_df = run_trades_simulation(merged_df, start_date, end_date, config, period_cash)
    return portfolio_df, positions_df, merged_df

if __name__ == "__main__":
    s3 = boto3.client('s3')
    strategy_theme = "invALERTS_cls" 
    backtest_configs = [
            {
            "put_pct": 1, 
            "spread_search": "1:4",
            "aa": 0,
            "risk_unit": 35,
            "portfolio_pct": .2,            
            "model": "CDVOLVARVC2",
            "vc_level":"100+120+140+500",
            "capital_distributions": ".33,.33,.33",
            "portfolio_cash": 60000,
            "scaling": "dynamicscale",
            "volatility_threshold": 0.4,
            "model_type": "cls",
            "user": "cm3",
            "threeD_vol": "return_vol_5D",
            "oneD_vol": "return_vol_5D",
            "dataset": "CDVOLBF3-6PE2",
            "spread_length": 3,
            "reserve_cash": 5000,
            "days": '23',
            "scale": "FIX",
            "reup": "daily",
            "IDX": False,
            "frequency": "15",
            "holiday_weeks": False,
            }
# {
#             "put_pct": 1, 
#             "spread_search": "0:3",
#             "aa": 0,
#             "risk_unit": .00825,
#             "model": "CDVOLVARVC",
#             "vc_level":"100+300+500",
#             "portfolio_cash": 100000,
#             "scaling": "dynamicscale",
#             "volatility_threshold": 0.4,
#             "model_type": "cls",
#             "user": "cm3",
#             "threeD_vol": "return_vol_10D",
#             "oneD_vol": "return_vol_5D",
#             "dataset": "CDVOLBF3-6TRIM",
#             "spread_length": 3,

#         },
    ]

    models_tested = []
    error_models = []
    nowstr = datetime.now().strftime("%Y%m%d")

    ## TREND STRATEGIES ONLY
    strategies = ["CDBFC_1D:1","CDBFP_1D:1"]    
    weeks = []

    for config in backtest_configs:
            trading_strat = f"{config['user']}-{nowstr}-24-modelCDVOL_dwnsdVOL:{config['model']}_1D=23_{config['dataset']}_vol{config['volatility_threshold']}_vc{config['vc_level']}_{config['scaling']}_sssl{config['spread_search']}:{config['spread_length']}"
            week = "2024-06-03"
            starting_cash = config['portfolio_cash']                
            start_date = week.replace("-","/")
            end_dt = datetime.strptime(week, '%Y-%m-%d') + timedelta(days=13)
            end_date = end_dt.strftime("%Y/%m/%d")
            start_str = start_date.split("/")[1] + start_date.split("/")[2]
            end_str = end_date.split("/")[1] + end_date.split("/")[2]

            print(f"Starting {trading_strat} at {datetime.now()} for {start_date} to {end_date} with ${starting_cash}")
            portfolio_df, positions_df, full_df = backtest_orchestrator(start_date, end_date,file_name=week,strategies=strategies,local_data=False, config=config, period_cash=starting_cash)
            starting_cash = portfolio_df['portfolio_cash'].iloc[-1]
            s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports_weekly/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
            s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports_weekly/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
            s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports_weekly/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
            print(f"Done with {trading_strat} at {datetime.now()}!")
            models_tested.append(f'{trading_strat}${config["portfolio_cash"]}_{config["risk_unit"]}')

            print(f"Completed all models at {datetime.now()}!")
            print(models_tested)
            print("Errors:")
            print(error_models)