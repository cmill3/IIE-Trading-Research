from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from helpers.constants import YEAR_CONFIG
import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import helpers.portfolio_simulation as portfolio_sim
import warnings
from backtest_config import backtest_configs
import concurrent.futures
import os


warnings.filterwarnings("ignore")
bucket_name = 'icarus-research-data'  #s3 bucket name
holiday_weeks = ['2022-07-05','2022-11-21','2022-12-26','2023-11-20','2023-07-03']


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

def backtest_orchestrator(start_date,end_date,file_names,strategies,local_data,config,period_cash):
    #  build_backtest_data(file_names[0],strategies,config)

    # if not local_data:
        # cpu_count = os.cpu_count()
        # # build_backtest_data(file_names[0],strategies,config)
        # with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        #     # Submit the processing tasks to the ThreadPoolExecutor
        #     processed_weeks_futures = [executor.submit(build_backtest_data,file_name,strategies,config) for file_name in file_names]

        # # Step 4: Retrieve the results from the futures
        # processed_weeks_results = [future.result() for future in processed_weeks_futures]

        # merged_positions = []
        # for week_results in processed_weeks_results:
        #     merged_positions.extend(week_results)

        # # merged_df = pd.DataFrame.from_dict(merged_positions)
        # # merged_df.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/merged_positions.csv', index=False)
    # else:
    #     merged_positions = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/merged_positions.csv')
    #     merged_positions = merged_positions.to_dict('records')
    all_trades = []
    for file_name in file_names:
        if config['holiday_weeks'] == False:
            if file_name in holiday_weeks:
                continue

        trades = build_backtest_data(file_name,strategies,config)
        all_trades.append(trades)


    full_df = pd.concat(all_trades)
    portfolio_df, positions_df = run_trades_simulation(full_df, start_date, end_date, config, period_cash)
    return portfolio_df, positions_df, full_df

if __name__ == "__main__":
    s3 = boto3.client('s3')
    strategy_theme = "invALERTS_cls" 
    
    models_tested = []
    error_models = []
    nowstr = datetime.now().strftime("%Y%m%d")


    ## TREND STRATEGIES ONLY
    strategies = [
        # "CDBFC:3","CDBFP:3",
        "CDBFC_1D:1","CDBFP_1D:1"
        ]    

    years = [
        'twenty4',
        ]
    backtest_configs = [
        {
            "put_pct": 1, 
            "spread_search": "2:4",
            "spread_length": 2,
            "aa": 0,
            "risk_unit": 35,
            "portfolio_pct": .2,
            "model": "CDVOLVARVC2",
            "vc_level":"100+120+140+300",
            "capital_distributions": ".40,.60",
            "portfolio_cash": 60000,
            "volatility_threshold": 1,
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "dataset": "CDVOLBF3-6PE2",
            "reserve_cash": 5000,
            "days": "23",
            "scale": "FIX",
            "divisor": .75,
            "reup": "daily",
            "IDX": False,
            "frequency": "15",
            "holiday_weeks": False,
        },
        ]
    for config in backtest_configs:
        for year in years:
            starting_cash = config['portfolio_cash']
            year_data = YEAR_CONFIG[year]
            trading_strat = f"{config['user']}/{nowstr}-{year_data['year']}:{config['aa']}:_{config['dataset']}_{config['holiday_weeks']}_{config['model']}_CD{config['capital_distributions']}_vol{config['volatility_threshold']}_vc{config['vc_level']}_sssl{config['spread_search']}:{config['spread_length']}"
            for month in year_data['months']:
                try:
                    start_dt = month[0]
                    end_date = month[-1]

                    start_date = start_dt.replace("-","/")
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=7)
                    end_date = end_dt.strftime("%Y/%m/%d")
                    start_str = start_date.split("/")[1] + start_date.split("/")[2]
                    end_str = end_date.split("/")[1] + end_date.split("/")[2]

                    print(f"Starting {trading_strat} at {datetime.now()} for {start_date} to {end_date} with ${starting_cash}")
                    portfolio_df, positions_df, full_df = backtest_orchestrator(start_date, end_date,file_names=month,strategies=strategies,local_data=False, config=config, period_cash=starting_cash)
                    starting_cash = portfolio_df['portfolio_cash'].iloc[-1]
                    s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
                    s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
                    s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
                    print(f"Done with {trading_strat} at {datetime.now()}!")
                except Exception as e:
                    print(f"Error: {e} for {trading_strat}")
                    error_models.append(f"Error: {e} for {trading_strat}")
                    continue
            models_tested.append(f'{trading_strat}${config["portfolio_cash"]}_{config["risk_unit"]}')

        print(f"Completed all models at {datetime.now()}!")
        print(models_tested)
        print("Errors:")
        print(error_models)




