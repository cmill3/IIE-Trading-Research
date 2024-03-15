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


bucket_name = 'icarus-research-data'  #s3 bucket name
object_keybase = 'training_datasets/expanded_1d_datasets/' #s3 key not including date, date is added in pullcsv func

def build_backtest_data(file_name,strategies,config):
    full_purchases_list = []
    full_positions_list = []
    full_sales_list = []

    dfs = []
    for strategy in strategies:
        try: 
            name, prediction_horizon = strategy.split(":")
            data = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{config["dataset"]}/{name}/{file_name}.csv')
            data['prediction_horizon'] = prediction_horizon
            dfs.append(data)
        except Exception as e:
            print(f"Error: {e} for {strategy} on {file_name}")
            continue
    
    backtest_data = pd.concat(dfs,ignore_index=True)
    # backtest_data = backtest_data[backtest_data['probabilities'] > config['probability']]
    if config['model_type'] == "reg":
        predictions = helper.configure_regression_predictions(backtest_data,config)
        filtered_by_date = helper.configure_trade_data(predictions,config)
    elif config['model_type'] == "cls":
        predictions = backtest_data.loc[backtest_data['predictions'] == 1]
        filtered_by_date = helper.configure_trade_data(predictions,config)
    
    ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
    positions_list = back_tester.simulate_trades_invalerts(filtered_by_date,config)
    full_positions_list.extend(positions_list)

    return positions_list

def run_trades_simulation(full_positions_list,start_date,end_date,config,period_cash):
    full_date_list = helper.create_portfolio_date_list(start_date, end_date)
    if config['scaling'] == "dynamicscale":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio_DS(
            full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
            config=config,results_dict_func=helper.extract_results_dict
            )
    elif config['scaling'] == "steadyscale":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio(
            full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
            config=config,results_dict_func=helper.extract_results_dict
            )
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df

def backtest_orchestrator(start_date,end_date,file_names,strategies,local_data,config,period_cash):
    #  build_backtest_data(file_names[0],strategies,config)

    if not local_data:
        cpu_count = os.cpu_count()
        # build_backtest_data(file_names[0],strategies,config)
        with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
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

    full_df = pd.DataFrame.from_dict(merged_positions)
    portfolio_df, positions_df = run_trades_simulation(merged_positions, start_date, end_date, config, period_cash)
    return portfolio_df, positions_df, full_df

if __name__ == "__main__":
    s3 = boto3.client('s3')
    strategy_theme = "invALERTS_cls" 
    backtest_configs = [
# {
#             "put_pct": 1, 
#             "spread_adjustment": 1,
#             "aa": 0,
#             "risk_unit": .009,
#             "model": "CDVOLAGG",
#             "vc_level":500,
#             "portfolio_cash": 10000,
#             "scaling": "dynamicscale",
#             "volatility_threshold": 0.5,
#             "model_type": "cls",
#             "user": "cm3",
#             "threeD_vol": "return_vol_10D",
#             "oneD_vol": "return_vol_5D",
#             "dataset": "CDVOLBF3-6",
#             "spread_length": 2,

#         },
{
            "put_pct": 1, 
            "spread_adjustment": 0,
            "aa": 0,
            "risk_unit": .009,
            "model": "CDVOLVARVC",
            "vc_level":"100/300/450",
            "portfolio_cash": 10000,
            "scaling": "dynamicscale",
            "volatility_threshold": 0.5,
            "model_type": "cls",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "oneD_vol": "return_vol_5D",
            "dataset": "CDVOLBF3-6",
            "spread_length": 3,

        },
    ]
    
    models_tested = []
    error_models = []
    nowstr = datetime.now().strftime("%Y%m%d")


    ## TREND STRATEGIES ONLY
    strategies = ["CDBFC:3","CDBFP:3","CDBFC_1D:1","CDBFP_1D:1"]    
    years = ['twenty1','twenty2','twenty3']

    for config in backtest_configs:
        for year in years:
            starting_cash = config['portfolio_cash']
            year_data = YEAR_CONFIG[year]
            starting_cash = config['portfolio_cash']
            trading_strat = f"{config['user']}-{nowstr}-{year_data['year']}-modelCDVOL_dwnsdVOL:{config['model']}_{config['dataset']}_vol{config['volatility_threshold']}_vc{config['vc_level']}_{config['scaling']}_sasl{config['spread_adjustment']}:{config['spread_length']}"
            for month in year_data['months']:
                if year_data['year'] == '21':
                    config['risk_unit'] = .006
                elif year_data['year'] == '22':
                    config['risk_unit'] = .0055
                elif year_data['year'] == '23':
                    config['risk_unit'] = .005
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


