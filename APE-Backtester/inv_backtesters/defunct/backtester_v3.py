from datetime import datetime, timedelta
import pandas as pd
import numpy as np
# import holidays
import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import helpers.portfolio_simulation as portfolio_sim
import warnings
import concurrent.futures
import os
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
        name, prediction_horizon = strategy.split(":")
        data = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{config["dataset"]}/{name}/{file_name}.csv')
        data['prediction_horizon'] = prediction_horizon
        dfs.append(data)
    
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
    if config['pos_limit'] == "poslimit":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio_poslimit(
            full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
            config=config
            )
    elif config['pos_limit'] == "noposlimit":
        portfolio_df, passed_trades_df, positions_taken, positions_dict = portfolio_sim.simulate_portfolio(
            full_positions_list, full_date_list,portfolio_cash=period_cash, risk_unit=config['risk_unit'],put_adjustment=config['put_pct'],
            config=config
            )
    positions_df = pd.DataFrame.from_dict(positions_taken)
    return portfolio_df, positions_df

def backtest_orchestrator(start_date,end_date,file_names,strategies,local_data,config,period_cash):
    #  build_backtest_data(file_names[0],strategies,config)

    if not local_data:
        cpu_count = os.cpu_count()
        # build_backtest_data(file_names[0],strategies,config)
        with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
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
     '2023-09-11', '2023-09-18', '2023-09-25', '2023-10-02', '2023-10-09', '2023-10-16', '2023-10-23', '2023-10-30',
     '2023-11-06', '2023-11-13', '2023-11-20', '2023-11-27', '2023-12-04', '2023-12-11', '2023-12-18', '2023-12-25'
     ]

    q1 = ['2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
     '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27']
    q2 = ['2023-03-06', '2023-03-13', '2023-03-20', 
     '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
     '2023-05-22', '2023-05-29']
    q3 = ['2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26', '2023-07-03', '2023-07-10', 
     '2023-07-17', '2023-07-24', '2023-07-31', '2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28','2023-09-04', '2023-09-11']
    q4 =  ['2023-09-18', '2023-09-25','2023-10-02', '2023-10-09', '2023-10-16', 
           '2023-10-23', '2023-10-30',
     '2023-11-06', '2023-11-13', '2023-11-20', '2023-11-27', '2023-12-04', '2023-12-11', '2023-12-18'
     ]

    backtest_configs = [
        # {
        #     "put_pct": 1, 
        #     "spread_adjustment": 0,
        #     "aa": 0,
        #     "risk_unit": .004,
        #     "model": "stdclsAGG",
        #     "vc_level":"300",
        #     "portfolio_cash": 100000,
        #     "pos_limit": "poslimit",
        #     "volatility_threshold": 1,
        #     "model_type": "cls",
        #     "user": "cm3",
        #     "threeD_vol": "return_vol_10D",
        #     "oneD_vol": "return_vol_5D",
        #     "dataset": "TL15"
        # },
        {
            "put_pct": 1, 
            "spread_adjustment": 0,
            "aa": 0,
            "risk_unit": .0035,
            "model": "stdclsAGG",
            "vc_level":"300",
            "portfolio_cash": 100000,
            "pos_limit": "poslimit",
            "volatility_threshold": 1,
            "model_type": "cls",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "oneD_vol": "return_vol_5D",
            "dataset": "TL15"
        },
        {
            "put_pct": 1, 
            "spread_adjustment": 0,
            "aa": 0,
            "risk_unit": .002,
            "model": "stdclsAGG",
            "vc_level":"300",
            "portfolio_cash": 100000,
            "pos_limit": "poslimit",
            "volatility_threshold": 1,
            "model_type": "cls",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "oneD_vol": "return_vol_5D",
            "dataset": "TL15"
        }
]
    
    time_periods = [q1,q2,q3,q4]
    models_tested = []
    error_models = []
    nowstr = datetime.now().strftime("%Y%m%d")

    # strategies = ["IDXC:3","IDXP:3","IDXC_1D:1","IDXP_1D:1","MA:3","MAP:3","MA_1D:1","MAP_1D:1","GAIN_1D:1","GAINP_1D:1","GAIN:3","GAINP:3","LOSERS:3","LOSERS_1D:1","LOSERSC:3","LOSERSC_1D:1","VDIFFC:3","VDIFFC_1D:1","VDIFFP_1D:1","VDIFFP:3"]
    # strategies = ["MA:3","MAP:3","MA_1D:1","MAP_1D:1","VDIFFC:3","VDIFFC_1D:1","VDIFFP_1D:1","VDIFFP:3"]
    # strategies = ["MA:3","MAP:3","MA_1D:1","MAP_1D:1","GAIN_1D:1","GAINP_1D:1","GAIN:3","GAINP:3","LOSERS:3","LOSERS_1D:1","LOSERSC:3","LOSERSC_1D:1"]


    # for config in backtest_configs:
    #     config['risk_unit'] = .0008
    #     trading_strat = f"{config['user']}-{nowstr}-modelVOLTRENDMA_dwnsdVOL:{config['model']}_{config['pos_limit']}_{config['vc_level']}_vol{config['volatility_threshold']}"
    #     for time in time_periods:
    #         try:
    #             start_dt = time[0]
    #             end_date = time[-1]

    #             start_date = start_dt.replace("-","/")
    #             end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=7)
    #             end_date = end_dt.strftime("%Y/%m/%d")
    #             start_str = start_date.split("/")[1] + start_date.split("/")[2]
    #             end_str = end_date.split("/")[1] + end_date.split("/")[2]

    #             print(f"Starting {trading_strat} at {datetime.now()}")
    #             portfolio_df, positions_df, full_df = backtest_orchestrator(start_date, end_date,file_names=time,strategies=strategies,local_data=False, config=config) 
    #             s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
    #             s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
    #             s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
    #             print(f"Done with {trading_strat} at {datetime.now()}!")
    #         except Exception as e:
    #             print(f"Error: {e} for {trading_strat}")
    #             error_models.append(f"Error: {e} for {trading_strat}")
    #             continue
    #     models_tested.append(trading_strat)

    # print(f"Completed all models at {datetime.now()}!")
    # print(models_tested)
    # print("Errors:")
    # print(error_models)

    ## TREND STRATEGIES ONLY
    # strategies = ["GAIN:3","GAINP:3","LOSERS:3","LOSERSC:3","GAIN_1D:1","GAINP_1D:1","LOSERS_1D:1","LOSERSC_1D:1"]

    for config in backtest_configs:
        trading_strat = f"{config['user']}-{nowstr}-modelVOLTREND_dwnsdVOL:{config['model']}_{config['pos_limit']}_{config['dataset']}_vol{config['volatility_threshold']}"
        starting_cash = config['portfolio_cash']
        for time in time_periods:
            # try:
                start_dt = time[0]
                end_date = time[-1]

                start_date = start_dt.replace("-","/")
                end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=7)
                end_date = end_dt.strftime("%Y/%m/%d")
                start_str = start_date.split("/")[1] + start_date.split("/")[2]
                end_str = end_date.split("/")[1] + end_date.split("/")[2]

                print(f"Starting {trading_strat} at {datetime.now()}")
                portfolio_df, positions_df, full_df = backtest_orchestrator(start_date, end_date,file_names=time,strategies=strategies,local_data=False, config=config,period_cash=starting_cash) 
                s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
                s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
                s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
                print(f"Done with {trading_strat} at {datetime.now()}!")
            # except Exception as e:
            #     print(f"Error: {e} for {trading_strat}")
            #     error_models.append(f"Error: {e} for {trading_strat}")
            #     continue
        models_tested.append(trading_strat)

    print(f"Completed all models at {datetime.now()}!")
    print(models_tested)
    print("Errors:")
    print(error_models)

    ## TREND STRATEGIES ONLY
    # time_periods = [q1,q2,q3,q4]
    # strategies = ["GAIN_1D:1","GAINP_1D:1","LOSERS_1D:1","LOSERSC_1D:1"]

    # for config in backtest_configs:
    #     trading_strat = f"{config['user']}-{nowstr}-modelVOLTREND1DHIGH_dwnsdVOL:{config['model']}_{config['pos_limit']}_{config['vc_level']}_vol{config['volatility_threshold']}"
    #     for time in time_periods:
    #         try:
    #             start_dt = time[0]
    #             end_date = time[-1]

    #             start_date = start_dt.replace("-","/")
    #             end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=7)
    #             end_date = end_dt.strftime("%Y/%m/%d")
    #             start_str = start_date.split("/")[1] + start_date.split("/")[2]
    #             end_str = end_date.split("/")[1] + end_date.split("/")[2]

    #             print(f"Starting {trading_strat} at {datetime.now()}")
    #             portfolio_df, positions_df, full_df = backtest_orchestrator(start_date, end_date,file_names=time,strategies=strategies,local_data=False, config=config) 
    #             s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
    #             s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
    #             s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
    #             print(f"Done with {trading_strat} at {datetime.now()}!")
    #         except Exception as e:
    #             print(f"Error: {e} for {trading_strat}")
    #             error_models.append(f"Error: {e} for {trading_strat}")
    #             continue
    #     models_tested.append(trading_strat)

    # print(f"Completed all models at {datetime.now()}!")
    # print(models_tested)
    # print("Errors:")
    # print(error_models)


    # # ## IDX STRATEGIES ONLY
    # q4 =  ['2023-09-18', '2023-09-25','2023-10-02', '2023-10-09', '2023-10-16', 
    # #        '2023-10-23', '2023-10-30',
    # #  '2023-11-06', '2023-11-13', '2023-11-20', '2023-11-27', '2023-12-04', '2023-12-11', '2023-12-18'
    # ]
    # strategies = ["IDXC:3","IDXP:3","IDXC_1D:1","IDXP_1D:1"]
    # time_periods = [q1,q2,q3,q4]


    # for config in backtest_configs:
    #     config['risk_unit'] = .006
    #     trading_strat = f"{config['user']}-{nowstr}-modelVOLIDX_dwnsdVOL:{config['model']}_{config['pos_limit']}_{config['vc_level']}_vol{config['volatility_threshold']}"
    #     for time in time_periods:
    #         try:
    #             start_dt = time[0]
    #             end_date = time[-1]

    #             start_date = start_dt.replace("-","/")
    #             end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=7)
    #             end_date = end_dt.strftime("%Y/%m/%d")
    #             start_str = start_date.split("/")[1] + start_date.split("/")[2]
    #             end_str = end_date.split("/")[1] + end_date.split("/")[2]

    #             print(f"Starting {trading_strat} at {datetime.now()}")
    #             portfolio_df, positions_df, full_df = backtest_orchestrator(start_date, end_date,file_names=time,strategies=strategies,local_data=False, config=config) 
    #             s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
    #             s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
    #             s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
    #             print(f"Done with {trading_strat} at {datetime.now()}!")
    #         except Exception as e:
    #             print(f"Error: {e} for {trading_strat}")
    #             error_models.append(f"Error: {e} for {trading_strat}")
    #             continue
    #     models_tested.append(trading_strat)

    # print(f"Completed all models at {datetime.now()}!")
    # print(models_tested)
    # print("Errors:")
    # print(error_models)


