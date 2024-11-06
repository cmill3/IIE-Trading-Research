import pandas as pd
from helpers import *
import ast
import warnings
from helpers import *
import concurrent.futures
warnings.filterwarnings("ignore")


def pull_data(strategy, years):
    all_files = []
    week_dfs = []

    for year in years:
        year_data = YEAR_CONFIG[year]
        print(year_data)
        for file_name in year_data['all_files']:
            print(year_data['all_files'])
            all_files.append(file_name)

    for file_name in all_files:
        try:
            week_df = pd.read_csv(f"/Users/charlesmiller/Documents/backtesting_data/TREND55-ALLSEV/{strategy}/{file_name}.csv")
            week_dfs.append(week_df)
        except FileNotFoundError:
            print(f"File {file_name} not found")
            continue

    return week_dfs

def dataset_creation_orchestrator(week_df):
    option_price_analytics_dfs = []
    failed_contracts_list = []
    for idx, row in week_df.iterrows():
        # try:
        contract_df, failed_contracts = build_contract_df(row)
        if len(failed_contracts) > 0:
            failed_contracts_list.extend(failed_contracts)
        if contract_df.empty:   
            continue
        option_price_analytics_dfs.append(contract_df)
        # except Exception as e:
        #     print(e)
        #     print(row['symbol'])
        #     print(idx)
        #     print()
        #     continue
    fopa = pd.concat(option_price_analytics_dfs)
    return fopa, failed_contracts_list


def build_contract_df(row):
    failed_contracts = []
    contract = ast.literal_eval(row['contracts'])
    last_price = get_last_price(row)
    row['alert_price'] = last_price
    if len(contract) == 0:
        return pd.DataFrame()
    ## turn a list into a df
    contract_df = pd.DataFrame(contract)
    contract_df.rename(columns={0:'contract_name'}, inplace=True)
    contract_df[['strike','expiry']] = contract_df['contract_name'].apply(lambda x: extract_strike_price(x,row['side']))
    ## find index of the contract with the strike price greater than row['alert_price']
    expiries = ast.literal_eval(row['expiries'])
    contract_dfs = []
    for expiry in expiries:
        expiry_df = contract_df[contract_df['expiry'] == expiry].reset_index(drop=True)
        if row['side'] == 'C':
            select_contracts = expiry_df[expiry_df['strike'] > row['alert_price']]
            select_contracts.sort_values(by='strike', ascending=True, inplace=True)
        elif row['side'] == 'P':
            select_contracts = expiry_df[expiry_df['strike'] < row['alert_price']]
            select_contracts.sort_values(by='strike', ascending=False, inplace=True)
        idx_break = select_contracts.index[0]
        select_contracts = expiry_df.iloc[(idx_break-1):(idx_break+4)]
        # if len(select_contracts) < 5:
        #     print(contract_df)
        #     print(expiry_df)
        #     print(expiry)
        #     print(row['alert_price'])   
        contract_dfs.append(select_contracts)
    for contract_df in contract_dfs:
        # print("LEN CONTRACT DF 3", len(contract_df))
        try:
            contract_df[["max_price","min_price","max_time_diff","min_time_diff","open_price","median_price_appreication"]] = contract_df.apply(lambda x: calculate_contract_price_statistics(x,row), axis=1, result_type='expand')
            contract_df['max_appreciation'] = contract_df['max_price'] - contract_df['open_price']/contract_df['open_price']
            contract_df['max_depreciation'] = contract_df['min_price'] - contract_df['open_price']/contract_df['open_price']
            contract_df['strategy'] = row['strategy']
            contract_df['side'] = row['side']
            contract_df['probability'] = row['probabilities']
            contract_df['label'] = row['label']
            contract_df['date'] = row['dt']
            contract_df['hour'] = row['hour']
            contract_df['minute'] = row['minute']
            contract_df.reset_index(drop=True, inplace=True)
            contract_df['spread_position'] = contract_df.index
        except Exception as e:
            print(e)
            print(contract_df)
            print(row)
            print()
            failed_contracts.append(contract_df)
            continue
    full_contract_df = pd.concat(contract_dfs)
    return full_contract_df, failed_contracts

def categorize_week(row):
    if row['symbol'] in ['SPY','QQQ','IWM']:
        if row['day_of_week'] == 1:
            return 'wk0'
        else:
            return 'wk1'
    if row['day_of_week'] == 'Monday':
        if row['days_to_expiry'] == 4:
            return 'wk0'
        else:
            return 'wk1'
    elif row['day_of_week'] == 'Tuesday':
        if row['days_to_expiry'] == 3:
            return 'wk0'
        else:
            return 'wk1'
    elif row['day_of_week'] == 'Wednesday':
        if row['days_to_expiry'] == 2:
            return 'wk0'
        else:
            return 'wk1'
    elif row['day_of_week'] == 'Thursday':
        if row['days_to_expiry'] == 1:
            return 'wk0'
        else:
            return 'wk1'
    elif row['day_of_week'] == 'Friday':
        if row['days_to_expiry'] == 0:
            return 'wk0'
        elif row['days_to_expiry'] == 7:
            return 'wk1'
        else:
            return 'wk2'
        

if __name__ == "__main__":
    strategies = [
        'CDLOSEC_2H',
        'CDLOSEP_2H',
        'CDvDIFFC_2H',
        'CDvDIFFP_2H',
        'CDGAINC_2H',
        'CDGAINP_2H']
    for strategy in strategies:
        week_dfs_list = pull_data(strategy, ['twenty4','twenty3','twenty2'])
        print("STARTING STRATEGY", strategy) 
        test_weeks = [week_dfs_list[-5],week_dfs_list[93],week_dfs_list[39]]
        processed_dfs = []
        failed_dfs = []
        # for test_week in test_weeks:
        #     print(f"STARTING WEEK {test_week['dt'].iloc[0]}")
        #     merged_df, failed_contracts = dataset_creation_orchestrator(test_week)
        #     test_dfs.append(merged_df)
        #     failed_dfs.append(failed_contracts)
        # ## parallelize build_contract_df using concurrent futures
        with concurrent.futures.ProcessPoolExecutor(max_workers=36) as executor:
            # Submit the processing tasks to the ThreadPoolExecutor
            processed_weeks_futures = [executor.submit(dataset_creation_orchestrator,week_df) for week_df in test_weeks]
        print("SUBMITTED FUTURES")
        print(processed_weeks_futures)

        for future in concurrent.futures.as_completed(processed_weeks_futures):
            fopa, failed_contracts = future.result()
            processed_dfs.append(fopa)
            failed_dfs.extend(failed_contracts)

        # Combine all processed data
        final_processed_data = pd.concat(processed_dfs, ignore_index=True)
        final_failed_contracts = pd.concat(failed_dfs, ignore_index=True)
        
        final_processed_data.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/research_resources/APE-Research/APE-Backtester/inv_backtesters/analysis/alerts_analysis/twoH_all_seventy/{strategy}.csv', index=False)
        final_failed_contracts.to_csv(f'/Users/charlesmiller/Documents/Code/IIE/research_resources/APE-Research/APE-Backtester/inv_backtesters/analysis/alerts_analysis/twoH_all_seventy/{strategy}_failed.csv', index=False)


    
    
