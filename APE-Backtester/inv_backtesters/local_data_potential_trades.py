import boto3
import helpers.backtest_functions as back_tester
import helpers.backtrader_helper as helper
import pandas as pd
import concurrent.futures
import pandas_market_calendars as mcal
import numpy as np
import ast
from datetime import datetime, timedelta
import numpy as np
import helpers.backtrader_helper as helper
import helpers.portfolio_simulation as portfolio_sim
import warnings
import concurrent.futures


warnings.filterwarnings("ignore")
bucket_name = 'icarus-research-data'  #s3 bucket name

s3 = boto3.client('s3')
nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

def pull_potential_trades(run_info):
    dfs = []
    for strategy in run_info['strategies']:
        for date in run_info['dates']:
            dt = datetime.strptime(date, '%Y/%m/%d')
            day_of_week = dt.weekday()
            for hour in ["10","11","12","13","14","15"]:
                key = f"invalerts_potential_trades/PROD_VAL/{strategy}/{date}/{hour}.csv"
                try:
                    contracts = s3.get_object(Bucket="inv-alerts-trading-data", Key=key)
                except Exception as e:
                    print(f"Error pulling data: {e} for {strategy}")
                    return []
                contracts = pd.read_csv(contracts['Body'])
                contracts['hour'] = hour
                contracts['day_of_week'] = day_of_week
                if strategy in ["CDBFC_1D","CDBFP_1D"]:
                    contracts['prediction_horizon'] = "1"
                else:
                    contracts['prediction_horizon'] = "3"
                dfs.append(contracts)
    contracts = pd.concat(dfs,ignore_index=True)
    contracts['probabilities'] = contracts['classifier_prediction'].astype(int)
    trade_data = contracts[contracts['classifier_prediction'] > 0.5]
    trade_data['trade_details1wk'] = trade_data['trade_details1wk'].apply(lambda x: ast.literal_eval(x))
    trade_data['num_contracts'] = trade_data['trade_details1wk'].apply(lambda x: len(x))
    trade_data = trade_data.loc[trade_data['num_contracts'] > 0]
    return trade_data



def build_backtest_data(run_info,config):
    predictions = pull_potential_trades(run_info)
    filtered_by_date = helper.configure_trade_data(predictions,config)
    ## What we will do is instead of simulating one trade at a time we will do one time period at a time and then combine and create results then.
    positions_list = back_tester.simulate_trades_invalerts_pt(filtered_by_date,config)
    return positions_list


def backtest_orchestrator(start_date,end_date,file_name,run_info,local_data,config,period_cash):
    positions_list = build_backtest_data(run_info,config)
    print(f"Positions list length: {len(positions_list)}")
    formatted_results = []
    for position in positions_list:
        print(position)
        print()
        results_dicts = helper.extract_results_dict_pt(position,config)
        formatted_results.append({'position_id':position['position_id'],"results":results_dicts})
    final_df = pd.DataFrame.from_dict(formatted_results)
    # portfolio_df, positions_df = run_trades_simulation(merged_df, start_date, end_date, config, period_cash)
    return final_df

if __name__ == "__main__":
    s3 = boto3.client('s3')
    strategy_theme = "invALERTS_cls" 
    backtest_configs = [
        {
            "put_pct": 1, 
            "spread_search": "2:4",
            "aa": 0,
            "risk_unit": .00825,
            "model": "CDVOLVARVC",
            "vc_level":"100+300+500+500",
            "portfolio_cash": 100000,
            "scaling": "dynamicscale",
            "volatility_threshold": 0.4,
            "model_type": "cls",
            "user": "cm3",
            "threeD_vol": "return_vol_10D",
            "oneD_vol": "return_vol_5D",
            # "dataset": "CDVOLBF3-6TRIM",
            "spread_length": 2,
        },
    ]

    models_tested = []
    error_models = []
    nowstr = datetime.now().strftime("%Y%m%d")

    ## TREND STRATEGIES ONLY
    weeks = []
    run_info = {
        "strategies": ['CDBFC_1D','CDBFP_1D',"CDBFC","CDBFP"],
        "dates": ["2024/04/29","2024/04/30","2024/05/01","2024/05/02"],
    }
    print(f"Starting {type(run_info['strategies'])} at {datetime.now()} for {run_info['dates']}")

    for config in backtest_configs:
            trading_strat = f"{config['user']}-POTTRADES:{config['model']}_vol{config['volatility_threshold']}_vc{config['vc_level']}_{config['scaling']}_sssl{config['spread_search']}:{config['spread_length']}"
            week = "2024-04-29"
            starting_cash = config['portfolio_cash']                
            start_date = week.replace("-","/")
            end_dt = datetime.strptime(week, '%Y-%m-%d') + timedelta(days=13)
            end_date = end_dt.strftime("%Y/%m/%d")
            start_str = start_date.split("/")[1] + start_date.split("/")[2]
            end_str = end_date.split("/")[1] + end_date.split("/")[2]

            print(f"Starting {trading_strat} at {datetime.now()} for {start_date} to {end_date} with ${starting_cash}")
            positions_df = backtest_orchestrator(start_date, end_date,file_name=week,run_info=run_info,local_data=False, config=config, period_cash=starting_cash)
            # starting_cash = portfolio_df['portfolio_cash'].iloc[-1]
            # s3.put_object(Body=portfolio_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports_weekly/pt_trades/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/portfolio_report.csv')
            s3.put_object(Body=positions_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports_weekly/pt_trades/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/positions_report.csv')
            # s3.put_object(Body=full_df.to_csv(), Bucket="icarus-research-data", Key=f'backtesting_reports_weekly/pt_trades/{strategy_theme}/{trading_strat}/{start_str}-{end_str}/{config["portfolio_cash"]}_{config["risk_unit"]}/all_positions.csv')
            print(f"Done with {trading_strat} at {datetime.now()}!")
            models_tested.append(f'{trading_strat}${config["portfolio_cash"]}_{config["risk_unit"]}')

            print(f"Completed all models at {datetime.now()}!")
            print(models_tested)
            print("Errors:")
            print(error_models)


