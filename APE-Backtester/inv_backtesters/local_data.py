import boto3
import helpers.backtest_functions as back_tester
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import pandas_market_calendars as mcal
import numpy as np
from helpers.constants import ONED_STRATEGIES, THREED_STRATEGIES

s3 = boto3.client('s3')
nyse = mcal.get_calendar('NYSE')
holidays = nyse.holidays()
holidays_multiyear = holidays.holidays

def add_contract_data_to_local(week,strategy_info,strategy,data_type):
        try:
            data, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key=f"backtesting_data/inv_alerts/{strategy_info['file_path']}", 
                                                    file_name = f"{week}.csv",prefixes=[strategy],time_span=strategy_info['time_span'])
            data.drop(columns=['Unnamed: 0.2','Unnamed: 0.1','Unnamed: 0'],inplace=True)
            data['side'] = strategy_info['side']
            data['contracts']= data.apply(lambda x: pull_contract_data(x),axis=1)
            data['expiries'] = data.apply(lambda x: generate_expiry_dates_row(x),axis=1)
            data.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/{data_type}/{strategy}/{week}.csv', index=False)
            print(f"Finished {strategy} for {week}")
        except Exception as e:
            print(f"Error: {e} for {strategy}")
    

        
def pull_contract_data(row):
    if row['symbol'] in ['SPY','QQQ','IWM']:
        date = row['date'].split(" ")[0]
        file_date = create_index_date(row['date'])
        year, month, day = file_date.strftime('%Y-%m-%d').split("-")
    else:
        date = row['date'].split(" ")[0]
        year, month, day = date.split("-")
    key = f"options_snapshot/{year}/{month}/{day}/{row['symbol']}.csv"
    try:
        contracts = s3.get_object(Bucket="icarus-research-data", Key=key)
    except Exception as e:
        print(f"Error pulling data: {e} for {row['symbol']}")
        return []
    contracts = pd.read_csv(contracts['Body'])
    try:
        contracts['date'] = contracts['symbol'].apply(lambda x: x[-15:-9])
        contracts['side'] = contracts['symbol'].apply(lambda x: x[-9])
        contracts['year'] = contracts['date'].apply(lambda x: f"20{x[:2]}")
        contracts['month'] = contracts['date'].apply(lambda x: x[2:4])
        contracts['day'] = contracts['date'].apply(lambda x: x[4:])
        contracts['date'] = contracts['year'] + "-" + contracts['month'] + "-" + contracts['day']
        expiry_dates = generate_expiry_dates(date,row['symbol'],row['strategy'])
        filtered_contracts = contracts[contracts['date'].isin(expiry_dates)]
        filtered_contracts = filtered_contracts[filtered_contracts['side'] == row['side']]
        contracts_list = filtered_contracts['symbol'].tolist()
    except Exception as e:
        print(f"Error: {e} for {row['symbol']}")
        print(row)
        print(row['symbol'])
        print(contracts)
        print()
        return []
    return contracts_list

def s3_to_local(file_name):
    data1, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice_top30FLowtuned", file_name = f"{file_name}.csv",prefixes=["gainers","vdiff_gainC"])
    data2, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_priceShortDH_top30FLtuned", file_name = f"{file_name}.csv",prefixes=["losers"])
    data3, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice3_top30FLLowtuned", file_name = f"{file_name}.csv",prefixes=["ma","maP"])
    data4, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice2_top30FLowtuned", file_name = f"{file_name}.csv",prefixes=["vdiff_gainP"])
    data = pd.concat([data1,data2,data3,data4],ignore_index=True)
    data.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/{file_name}.csv', index=False)

def generate_expiry_dates(date_str,symbol,strategy):
    if symbol in ['SPY','QQQ','IWM']:
        if strategy in ONED_STRATEGIES:
            day_of = add_weekdays(date_str,1,symbol)
            next_day = add_weekdays(date_str,2,symbol)
            return [day_of.strftime('%Y-%m-%d'),next_day.strftime('%Y-%m-%d')]
        elif strategy in THREED_STRATEGIES:
            day_of = add_weekdays(date_str,3,symbol)
            next_day = add_weekdays(date_str,4,symbol)
            return [day_of.strftime('%Y-%m-%d'),next_day.strftime('%Y-%m-%d')]
    else: 
        input_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Find the weekday of the input date (Monday is 0 and Sunday is 6)
        weekday = input_date.weekday()


    if weekday == 4:
        closest_friday = input_date + timedelta(days=7)
        following_friday = input_date + timedelta(days=14)
        return [closest_friday.strftime('%Y-%m-%d'), following_friday.strftime('%Y-%m-%d')]

    # Calculate days until the next Friday
    days_until_closest_friday = (4 - weekday) % 7
    days_until_following_friday = days_until_closest_friday + 7
    closest_friday = input_date + timedelta(days=days_until_closest_friday)
    following_friday = input_date + timedelta(days=days_until_following_friday)

    return [closest_friday.strftime('%Y-%m-%d'), following_friday.strftime('%Y-%m-%d')]

def create_index_date(date):
    str = date.split(" ")[0]
    dt = datetime.strptime(str, '%Y-%m-%d')
    wk_day = dt.weekday()
    monday = dt - timedelta(days=wk_day)
    monday_np = np.datetime64(monday)
    
    if monday_np in holidays_multiyear:
        monday = monday + timedelta(days=1)
    return monday

def generate_expiry_dates_row(row):
    date_str = row['date'].split(" ")[0]
    if row['symbol'] in ['SPY','QQQ','IWM']:
        if row['strategy'] in ONED_STRATEGIES:
            day_of = add_weekdays(date_str,1,row['symbol'])
            next_day = add_weekdays(date_str,2,row['symbol'])
            return [day_of.strftime('%y%m%d'),next_day.strftime('%y%m%d')]
        elif row['strategy'] in THREED_STRATEGIES:
            day_of = add_weekdays(date_str,3,row['symbol'])  
            next_day = add_weekdays(date_str,4,row['symbol'])
            return [day_of.strftime('%y%m%d'),next_day.strftime('%y%m%d')]
    else: 
        input_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Find the weekday of the input date (Monday is 0 and Sunday is 6)
        weekday = input_date.weekday()

    if weekday == 4:
        closest_friday = input_date + timedelta(days=7)
        following_friday = input_date + timedelta(days=14)

        return [closest_friday.strftime('%y%m%d'), following_friday.strftime('%y%m%d')]

    # Calculate days until the next Friday
    days_until_closest_friday = (4 - weekday) % 7
    days_until_following_friday = days_until_closest_friday + 7
    closest_friday = input_date + timedelta(days=days_until_closest_friday)
    following_friday = input_date + timedelta(days=days_until_following_friday)


    return [closest_friday.strftime('%y%m%d'), following_friday.strftime('%y%m%d')]

def add_weekdays(date,days,symbol):
    if type(date) == str:
        date = datetime.strptime(date, '%Y-%m-%d')
    # date = datetime.strptime(date, '%Y-%m-%d')
    while days > 0:
        date += timedelta(days=1)
        if date.weekday() < 5:
            days -= 1

    if symbol == "IWM":
        if date.weekday() in [1,3]:
            date += timedelta(days=1)
    return date

if __name__ == "__main__":
    strategy_info = {
        #  "IDXC_1d": {
        #       "file_path": 'TSSIM1PCR2_custHypP09',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        # "IDXP_1d": {
        #       "file_path": 'TSSIM1PCR1_custHypP079_2018',
        #       "time_span": 2,
        #       "side": "P"
        #  },
        #  "IDXP": {
        #       "file_path": 'TSSIMPCR2_custHypP115_2018',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        # "IDXC": {
        #       "file_path": 'TSSIM1PCR1_custHypP14_2018',
        #       "time_span": 4,
        #       "side": "C"
        #  },
        #  "VDIFFC_1d": {
        #       "file_path": 'TSSIM1_TL15_custHypP16',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        # "VDIFFP_1d": {
        #       "file_path": 'TSSIM1_TL15_custHypP16',
        #       "time_span": 2,
        #       "side": "P"
        #  },
        #  "VDIFFC": {
        #       "file_path": 'TSSIM1_TL15_custHypP25',
        #       "time_span": 4,
        #       "side": "C"
        #  },
        # "VDIFFP": {
        #       "file_path": 'TSSIM1_TL15_custHypP24',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        "GAIN_1d": {
              "file_path": 'GAIN_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.52',
              "time_span": 2,
              "side": "C"
         },
        "LOSERS_1d": {
              "file_path": 'LOSERS_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.48',
              "time_span": 2,
              "side": "P"
         },
         "GAIN": {
              "file_path": 'GAIN:RM220_TSSIM1_TL15-EXP_custHypTP0.52',
              "time_span": 4,
           
           "side": "C"
         },
        "LOSERS": {
              "file_path": 'LOSERS:RM220_TSSIM1_TL15-EXP_custHypTP0.48',
              "time_span": 4,
              "side": "P"
         },
         "GAINP_1d": {
              "file_path": 'GAINP_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.48',
              "time_span": 2,
              "side": "P"
         },
        "LOSERSC_1d": {
              "file_path": 'LOSERSC_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.52',
              "time_span": 2,
              "side": "C"
         },
         "GAINP": {
              "file_path": 'GAINP:RM220_TSSIM1_TL15-EXP_custHypTP0.48',
              "time_span": 4,
              "side": "P"
         },
        "LOSERSC": {
              "file_path": 'LOSERSC:RM220_TSSIM1_TL15-EXP_custHypTP0.52',
              "time_span": 4,
              "side": "C"
         },
         "MAP_1d": {
              "file_path": 'MAP_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.48',
              "time_span": 2,
              "side": "P"
         },
        "MA_1d": {
              "file_path": 'MA_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.52',
              "time_span": 2,
              "side": "C"
         },
         "MAP": {
              "file_path": 'MAP:RM220_TSSIM1_TL15-EXP_custHypTP0.48',
              "time_span": 4,
              "side": "P"
         },
        "MA": {
              "file_path": 'MA:RM220_TSSIM1_TL15-EXP_custHypTP0.52',
              "time_span": 4,
              "side": "C"
         }
        # "CDGAIN": {
        #       "file_path": 'TSSIM1:1_TL15-EXP_custHypTP0.45',
        #       "time_span": 4,
        #       "side": "C"
        #  },
        # "CDLOSE": {
        #       "file_path": 'TSSIM1:1_TL15-EXP_custHypTP0.55',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        #   "CDGAIN_1d": {
        #       "file_path": 'TSSIM1:1_TL15-EXP_custHypTP0.45',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        # "CDLOSE_1d": {
        #       "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 2,
        #       "side": "P"
        # "CDBFC": {
        #       "file_path": 'TSSIM1_IDX_custHypTP0.55',
        #       "time_span": 4,
        #       "side": "C"
        #  },
        # "CDBFP": {
        #       "file_path": 'TSSIM1_IDX_custHypTP0.47',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        #   "CDBFC_1D": {
        #       "file_path": 'TSSIM1_IDX_custHypTP0.55',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        # "CDBFP_1D": {
        #       "file_path": 'TSSIM1_IDX_custHypTP0.47',
        #       "time_span": 2,
        #       "side": "P"
        #  },
    }

    file_names = [
     '2023-01-02', '2023-01-09', '2023-01-16', '2023-01-23', 
     '2023-01-30', '2023-02-06', '2023-02-13', '2023-02-20', '2023-02-27', '2023-03-06', '2023-03-13', '2023-03-20', 
     '2023-03-27', '2023-04-03', '2023-04-10', '2023-04-17', '2023-04-24', '2023-05-01', '2023-05-08', '2023-05-15', 
     '2023-05-22', '2023-05-29', '2023-06-05', '2023-06-12', '2023-06-19', '2023-06-26', '2023-07-03', '2023-07-10', 
     '2023-07-17', '2023-07-24', '2023-07-31', '2023-08-07', '2023-08-14', '2023-08-21', '2023-08-28', '2023-09-04', 
     '2023-09-11', '2023-09-18', '2023-09-25', 
     '2023-10-02', '2023-10-09', '2023-10-16', '2023-10-23', '2023-10-30',
     '2023-11-06', '2023-11-13', '2023-11-20', '2023-11-27', '2023-12-04', '2023-12-11', '2023-12-18'
     ]
    data_type = 'TL15RM'
    
    # add_contract_data_to_local(file_names,strategy_info['GAIN'],"GAIN",'cls')
    
    for strategy in strategy_info:
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            # Submit the processing tasks to the ThreadPoolExecutor
            processed_weeks_futures = [executor.submit(add_contract_data_to_local,week,strategy_info[strategy],strategy,data_type) for week in file_names]
        # add_contract_data_to_local(file_names[0],strategy_info[strategy],strategy,data_type)

    # for week in file_names:
    #     for strategy in ['BFC','BFP','BFC_1D','BFP_1D']:
    #         df = pd.read_csv(f'/Users/charlesmiller/Documents/backtesting_data/{strategy}/2023-10-02.csv')
    #         print(f"num of columns for {strategy} in {week}: {(len(df.columns))}")
        

    #    "GAIN_1d": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
    #           "time_span": 2,
    #           "side": "C"
    #      },
    #     "LOSERS_1d": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.45',
    #           "time_span": 2,
    #           "side": "P"
    #      },
    #      "GAIN": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
    #           "time_span": 4,
    #           "side": "C"
    #      },
    #     "LOSERS": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.45',
    #           "time_span": 4,
    #           "side": "P"
    #      },
    #      "GAINP_1d": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.45',
    #           "time_span": 2,
    #           "side": "P"
    #      },
    #     "LOSERSC_1d": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
    #           "time_span": 2,
    #           "side": "C"
    #      },
    #      "GAINP": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.45',
    #           "time_span": 4,
    #           "side": "P"
    #      },
    #     "LOSERSC": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
    #           "time_span": 4,
    #           "side": "C"
    #      },
    #      "MAP_1d": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.45',
    #           "time_span": 2,
    #           "side": "P"
    #      },
    #     "MA_1d": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
    #           "time_span": 2,
    #           "side": "C"
    #      },
    #      "MAP": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.45',
    #           "time_span": 4,
    #           "side": "P"
    #      },
    #     "MA": {
    #           "file_path": 'TSSIM1_TL15-EXP_custHypTP0.55',
    #           "time_span": 4,
    #           "side": "C"
    #      }
    
        #      "GAIN_1d": {
        #       "file_path": 'GAIN_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        # "LOSERS_1d": {
        #       "file_path": 'LOSERS_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.45',
        #       "time_span": 2,
        #       "side": "P"
        #  },
        #  "GAIN": {
        #       "file_path": 'GAIN:RM220_TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 4,
        #       "side": "C"
        #  },
        # "LOSERS": {
        #       "file_path": 'LOSERS:RM220TSSIM1_TL15-EXP_custHypTP0.45',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        #  "GAINP_1d": {
        #       "file_path": 'GAINP_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.45',
        #       "time_span": 2,
        #       "side": "P"
        #  },
        # "LOSERSC_1d": {
        #       "file_path": 'LOSERSC_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        #  "GAINP": {
        #       "file_path": 'GAINP:RM220_TSSIM1_TL15-EXP_custHypTP0.45',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        # "LOSERSC": {
        #       "file_path": 'LOSERSC:RM220_TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 4,
        #       "side": "C"
        #  },
        #  "MAP_1d": {
        #       "file_path": 'MA_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.45',
        #       "time_span": 2,
        #       "side": "P"
        #  },
        # "MA_1d": {
        #       "file_path": 'MA_1d:RM220_TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 2,
        #       "side": "C"
        #  },
        #  "MAP": {
        #       "file_path": 'MAP:RM220_TSSIM1_TL15-EXP_custHypTP0.45',
        #       "time_span": 4,
        #       "side": "P"
        #  },
        # "MA": {
        #       "file_path": 'MA:RM220_TSSIM1_TL15-EXP_custHypTP0.55',
        #       "time_span": 4,
        #       "side": "C"
        #  }