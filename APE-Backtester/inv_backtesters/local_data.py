import boto3
import helpers.backtest_functions as back_tester
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
s3 = boto3.client('s3')

def add_contract_data_to_local(weeks,strategy_info):
    print(strategy_info)
    # dfs = []
    for week in weeks:
            try:
                data, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key=f"backtesting_data/inv_alerts/{strategy_info['file_path']}", 
                                                        file_name = f"{week}.csv",prefixes=[strategy],time_span=strategy_info['time_span'])
                # data.drop(columns=['Unnamed: 0.6','Unnamed: 0.3','Unnamed: 0.2','Unnamed: 0.1','Unnamed: 0'],inplace=True)
                data.drop(columns=['Unnamed: 0.2','Unnamed: 0.1','Unnamed: 0'],inplace=True)
                data['side'] = strategy_info['side']
                data['contracts']= data.apply(lambda x: pull_contract_data(x),axis=1)
                data['expiries'] = data['date'].apply(lambda x: generate_expiry_dates_row(x))
                data.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/{strategy}/{week}.csv', index=False)
            except Exception as e:
                print(f"Error: {e} for {strategy}")
                continue
    #         dfs.append(data)
    # data = pd.concat(dfs,ignore_index=True)
    

        
def pull_contract_data(row):
    date = row['date'].split(" ")[0]
    year, month, day = date.split("-")
    key = f"options_snapshot/{year}/{month}/{day}/{row['symbol']}.csv"
    try:
        contracts = s3.get_object(Bucket="icarus-research-data", Key=key)
    except Exception as e:
        print(f"Error: {e} for {row['symbol']}")
        return []
    contracts = pd.read_csv(contracts['Body'])
    try:
        contracts['date'] = contracts['symbol'].apply(lambda x: x[-15:-9])
        contracts['side'] = contracts['symbol'].apply(lambda x: x[-9])
        contracts['year'] = contracts['date'].apply(lambda x: f"20{x[:2]}")
        contracts['month'] = contracts['date'].apply(lambda x: x[2:4])
        contracts['day'] = contracts['date'].apply(lambda x: x[4:])
        contracts['date'] = contracts['year'] + "-" + contracts['month'] + "-" + contracts['day']
        expiry_dates = generate_expiry_dates(date)
        filtered_contracts = contracts[contracts['date'].isin(expiry_dates)]
        filtered_contracts = filtered_contracts[filtered_contracts['side'] == row['side']]
        contracts_list = filtered_contracts['symbol'].tolist()
    except Exception as e:
        print(f"Error: {e} for {row['symbol']}")
        print(row)
        print(contracts)
        return []
    return contracts_list

def s3_to_local(file_name):
    data1, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice_top30FLowtuned", file_name = f"{file_name}.csv",prefixes=["gainers","vdiff_gainC"])
    data2, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_priceShortDH_top30FLtuned", file_name = f"{file_name}.csv",prefixes=["losers"])
    data3, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice3_top30FLLowtuned", file_name = f"{file_name}.csv",prefixes=["ma","maP"])
    data4, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice2_top30FLowtuned", file_name = f"{file_name}.csv",prefixes=["vdiff_gainP"])
    data = pd.concat([data1,data2,data3,data4],ignore_index=True)
    data.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/{file_name}.csv', index=False)

def generate_expiry_dates(date_str):
    input_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Find the weekday of the input date (Monday is 0 and Sunday is 6)
    weekday = input_date.weekday()

    if weekday == 4:
        closest_friday = input_date + timedelta(days=7)
        following_friday = input_date + timedelta(days=14)
        closest_friday_str = closest_friday.strftime('%Y-%m-%d')
        following_friday_str = following_friday.strftime('%Y-%m-%d')
        return [closest_friday_str, following_friday_str]

    # Calculate days until the next Friday
    days_until_closest_friday = (4 - weekday) % 7
    days_until_following_friday = days_until_closest_friday + 7
    closest_friday = input_date + timedelta(days=days_until_closest_friday)
    following_friday = input_date + timedelta(days=days_until_following_friday)

    # Format the dates back to strings
    closest_friday_str = closest_friday.strftime('%Y-%m-%d')
    following_friday_str = following_friday.strftime('%Y-%m-%d')

    return [closest_friday_str, following_friday_str]

def generate_expiry_dates_row(date):
    date_str = date.split(" ")[0]
    input_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Find the weekday of the input date (Monday is 0 and Sunday is 6)
    weekday = input_date.weekday()

    if weekday == 4:
        closest_friday = input_date + timedelta(days=7)
        following_friday = input_date + timedelta(days=14)
        closest_friday_str = closest_friday.strftime('%y%m%d')
        following_friday_str = following_friday.strftime('%y%m%d')

        return [closest_friday_str, following_friday_str]

    # Calculate days until the next Friday
    days_until_closest_friday = (4 - weekday) % 7
    days_until_following_friday = days_until_closest_friday + 7
    closest_friday = input_date + timedelta(days=days_until_closest_friday)
    following_friday = input_date + timedelta(days=days_until_following_friday)

    # Format the dates back to strings
    closest_friday_str = closest_friday.strftime('%y%m%d')
    following_friday_str = following_friday.strftime('%y%m%d')

    return [closest_friday_str, following_friday_str]

if __name__ == "__main__":
    # all_dates = [
    #      '2022-01-03', '2022-01-10', '2022-01-17', '2022-01-24', '2022-01-31', '2022-02-07', '2022-02-14', '2022-02-21', 
    #      '2022-02-28', '2022-03-07', '2022-03-14', '2022-03-21', '2022-03-28', '2022-04-04', '2022-04-11', '2022-04-18', 
    #      '2022-04-25', '2022-05-02', '2022-05-09', '2022-05-16', '2022-05-23', '2022-05-30', '2022-06-06', '2022-06-13', 
    #      '2022-06-20', '2022-06-27', '2022-07-04', '2022-07-11', '2022-07-18', '2022-07-25', '2022-08-01', '2022-08-08', 
    #      '2022-08-15', '2022-08-22', '2022-08-29', '2022-09-05', '2022-09-12', '2022-09-19', '2022-09-26', '2022-10-03', 
    #      '2022-10-10', '2022-10-17', '2022-10-24', '2022-10-31', '2022-11-07', '2022-11-14', '2022-11-21', '2022-11-28', 
    #      '2022-12-05', '2022-12-12', '2022-12-19', '2022-12-26', '2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
    #      '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
    #      '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
    #      '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26', '2023-07-03', '2023-07-10', 
    #      '2023-07-17', '2023-07-24', '2023-07-31', '2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28', '2023-09-04', 
    #      '2023-09-11', '2023-09-18', '2023-09-25', '2023-10-02', '2023-10-09', '2023-10-16', '2023-10-23', '2023-10-30', 
    #      '2023-11-06'
    #      ]


    strategy_info = {
         "BFP_1D": {
              "file_path": "1D_TSSIM1_best231110_custHypP15_2018",
              "time_span": 2,
              "side": "P"
         },
        "BFC_1D": {
              "file_path": "1D_TSSIM1_best231110_custHypP15_2018",
              "time_span": 2,
              "side": "C"
         },
         "BFP": {
              "file_path": "TSSIM1_best231110_custHypP25_2018",
              "time_span": 4,
              "side": "P"
         },
        "BFC": {
              "file_path": "TSSIM1_best231110_custHypP25_2018",
              "time_span": 4,
              "side": "C"
         }
    }
    file_names = ['2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
         '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
         '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19']
    
    for strategy in strategy_info:
        add_contract_data_to_local(file_names,strategy_info[strategy])

    # for week in file_names:
    #     for strategy in ['BFC','BFP','BFC_1D','BFP_1D']:
    #         df = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{strategy}/2023-10-02.csv')
    #         print(f"num of columns for {strategy} in {week}: {(len(df.columns))}")



