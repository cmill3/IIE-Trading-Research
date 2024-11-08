from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json
import ast
# import holidays
import warnings
# import helpers.bf_strategies as ts
# import helpers.helper as helper
import helpers.polygon_helper as ph
import pytz
# from helpers.constants import ONED_STRATEGIES


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)


AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

def startbacktrader(Starting_Cash):
    Starting_Value = Starting_Cash
    return Starting_Value

def round_up_to_base(x, base=5):
    return x + (base - x) % base

def round_down_to_base(x, base=5):
    return x - (x % base)


def create_end_date(date, trading_days_to_add):
    #Trading days only
    while trading_days_to_add > 0:
        date += timedelta(days=1)
        weekday = date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1

    day = date.day
    month = date.month
    year = date.year
    date = datetime(year, month, day, 16, 0)
    return date

def create_end_date_local_data(date, trading_days_to_add):
    #Trading days only
    date = datetime.strptime(date, '%Y-%m-%d')
    # trading_days_to_add = add_days
    while trading_days_to_add > 0:
        date += timedelta(days=1)
        weekday = date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    
    return date

def create_end_date_tstamp(date, trading_days_to_add):
    #Trading days only
    # date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    # trading_days_to_add = add_days
    while trading_days_to_add > 0:
        date += timedelta(days=1)
        weekday = date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    
    return date

def s3_data_inv(bucket_name, object_key, prefixes):
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    rawdata = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(rawdata))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    df['contracts'] = df['contracts'].apply(lambda x: ast.literal_eval(x))
    # df['contracts_available'] = df['contracts'].apply(lambda x: len(x)>=12)
    return df

def create_results_dict(buy_dict, sell_dict,order_id):
    sell_dict['close_price'] = .98*sell_dict['close_price']
    buy_dict['open_price'] = 1.02*buy_dict['open_price']
    price_change = sell_dict['close_price'] - buy_dict['open_price']
    pct_gain = (price_change / buy_dict['open_price']) *100
    total_gain = (price_change*100)
    buy_dict['order_id'] = order_id
    sell_dict['order_id'] = order_id
    sell_dict['close_datetime'] = sell_dict['close_datetime'].strftime("%Y-%m-%d %H:%M")
    buy_dict['open_datetime'] = buy_dict['open_datetime'].strftime("%Y-%m-%d %H:%M")

    custom_sell = sell_dict.copy()
    custom_buy = buy_dict.copy()
    ## remove value from dict
    custom_sell.pop('close_datetime')
    custom_buy.pop('open_datetime')
    results_dict = {
                    "price_change":price_change, "pct_gain":pct_gain, "total_gain":total_gain,
                    "open_trade_dt": buy_dict['open_datetime'], "close_trade_dt": sell_dict['close_datetime'],
                    "sell_info": custom_sell, "buy_info": custom_buy,
                    }
    return results_dict

def create_options_aggs_inv(row,start_date,end_date,spread_length,config):
    options = []
    enriched_options_aggregates = {}
    expiries = ast.literal_eval(row['expiries'])

    # ASSIGNMENT ADJUSTMENT
    # threeD_cutoff, oneD_cutoff = map_assignment_adjustment(config['aa'])
    # if row['strategy'] in THREED_STRATEGIES:
    #         if start_date.weekday() > 2:
    #             expiry = expiries[1]
    #         else:
    #             expiry = expiries[0]
    # elif row['strategy'] in ONED_STRATEGIES:
    #     if start_date.weekday() > 4:
    #             expiry = expiries[0]
    #     else:
    #         expiry = expiries[1]
    # # if row['strategy'] in ["IDXC_1D","IDXP","IDXC","IDXP_1D"]:
    # #     expiry = expiries[config['aa']]
    # # else:
    # #     expiry = expiries[0]

    ##LOOK HERE AND ABOVE TO PUSH THE TRADES TO THE NEXT WEEK @ line 112 if we want to run Thursday with the next week contracts
    ##START DAY IS THE DAY THE ALERT CAME THROUGH, ADD SOMETHING FOR IF DAY = 3 (THURSDAY) THEN PUSH THE EXPIRY TO 1 WEEK OUT

    if row['symbol'] in ['SPY','IWM','QQQ']:
        expiry = expiries[1]
    else:
        if config['aa'] == 0:
            expiry = expiries[0]
        elif config['aa'] == 1:
            expiry = expiries[1]
    
    strike = row['symbol'] + expiry
    try:
        underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date,config['frequency'])
    except Exception as e:
        print(f"Error: {e} in underlying agg for {row['symbol']} of {row['strategy']}")
        try:
            underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date,config['frequency'])
        except Exception as e:
            print(f"Error: {e} in underlying agg for 2ND {row['symbol']} of {row['strategy']}")
            try: 
                underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date,config['frequency'])
            except Exception as e:
                print(f"Error: {e} in underlying agg for FINAL {row['symbol']} of {row['strategy']}")
                return [], []
            
    try:
        underlying_agg_data.rename(columns={'o':'underlying_price'}, inplace=True)
        open_price = underlying_agg_data['underlying_price'].values[0]
        contracts = ast.literal_eval(row['contracts'])
    except Exception as e:
        print(f"Error: {e} in evaluating contracts for {row['symbol']} of {row['strategy']}")
        print(row['contracts'])
        print(row['symbol'])
        print(start_date)
        print(end_date)
        print(underlying_agg_data)
        print("ANALYSIS NO UNDERLYING")
        print()
        return [], []
    filtered_contracts = [k for k in contracts if strike in k]
    if len(filtered_contracts) == 0:
        print(f"No contracts for {row['symbol']} of {row['strategy']}")
        try:
            strike2 = row['symbol'] + contracts[0][-15:-9]
            filtered_contracts = [k for k in contracts if strike2 in k]
            if len(filtered_contracts) == 0:
                print(f"No contracts 2nd try for {row['symbol']} of {row['strategy']}")
                print(contracts)
                print(strike)
                print(start_date)
                return [], []
        except Exception as e:
            print(f"Error: {e} in 2nd try for {row['symbol']} of {row['strategy']}")
            print(contracts)
            print(strike)
            print(start_date)
            return [], []
    options_df = build_options_df(filtered_contracts, row)
    ## SPREAD ADJUSTMENT
    # options_df = options_df.iloc[config['spread_adjustment']:]
    # spread_start, spread_end = config['spread_search'].split(':')
    # options_df = options_df.iloc[int(spread_start):int(spread_end)]
    for index,contract in options_df.iterrows():
        try:
            options_agg_data = ph.polygon_optiondata(contract['contract_symbol'], start_date, end_date,config['frequency'])
            enriched_df = pd.merge(options_agg_data, underlying_agg_data[['date', 'underlying_price']], on='date', how='left')
            enriched_df.dropna(inplace=True)
            enriched_options_aggregates[contract['contract_symbol']] = enriched_df
            options.append(contract)
            if len(options) > (spread_length+4):
                break
        except Exception as e:
            print(f"Error: {e} in options agg for {row['symbol']} of {row['strategy']}")
            print(contract)
            continue
    return enriched_options_aggregates, options

def create_volume_aggs_inv(row,start_date,end_date,options,config):
    volume_aggregates = {}
    for contract in options:
        try:
            volume_agg_data = ph.polygon_optiondata(contract['contract_symbol'], start_date, end_date,config['frequency'])
            volume_agg_data['volume_15ema'] = volume_agg_data['v'].ewm(span=15, adjust=False).mean()
            volume_aggregates[contract['contract_symbol']] = volume_agg_data['volume_15ema'].values[-1]
        except Exception as e:
            print(f"Error: {e} in volume agg for {row['symbol']} of {row['strategy']}")
            print(contract)
            continue
    print(volume_aggregates)
    return volume_aggregates


def create_options_aggs_pt(row,start_date,end_date,config):
    options = []
    enriched_options_aggregates = []

    try:
        underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date)
    except Exception as e:
        print(f"Error: {e} in underlying agg for {row['symbol']} of {row['strategy']}")
        underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date)
            
    underlying_agg_data.rename(columns={'o':'underlying_price'}, inplace=True)
    open_price = underlying_agg_data['underlying_price'].values[0]
    for contract in row['trade_details1wk']:
        try:
            options_agg_data = ph.polygon_optiondata(contract['contract_ticker'], start_date, end_date)
            enriched_df = pd.merge(options_agg_data, underlying_agg_data[['date', 'underlying_price']], on='date', how='left')
            enriched_df.dropna(inplace=True)
            enriched_df['quantity'] = contract['quantity']
            enriched_options_aggregates.append(enriched_df)
            options.append(contract)
        except Exception as e:
            print(f"Error: {e} in options agg for {row['symbol']} of {row['strategy']}")
            print(contract)
            continue
    return enriched_options_aggregates, options, open_price

def generate_datetime_range(start_date, end_date):
    delta = timedelta(minutes=15)
    current_date = start_date
    datetime_range = []

    while current_date <= end_date:
        datetime_range.append(current_date.strftime("%Y-%m-%dT%H:%M"))
        current_date += delta

    return datetime_range

def convert_lists_to_dicts_inv(trades_df, datetime_list):
    portfolio_dict = {}
    positions_dict = {}
    sales_dict = {}
    passed_trades_dict = {}
    for date in datetime_list:
        year = date.year
        month = date.month
        day = date.day
        hour = date.hour
        minute = date.minute
        dt = datetime(year,month,day,hour,minute)
        # dt = datetime.strptime(dt, "%Y-%m-%d %H:%M")
        portfolio_dict[dt] = {
            "contracts_purchased": [],
            "purchase_costs": 0,
            "contracts_sold": [],
            "sale_returns": 0,
            "portfolio_capital": 0,
            "trading_capital": 0,
            "active_holdings": [],
            "period_net_returns": 0,
            "open_positions_start": [],
            "open_positions_end": [],
        }
    trades_df['open_datetime'] = trades_df.apply(lambda x: f"{x['dt']} {x['hour']}:00", axis=1)
    for _,position in trades_df.iterrows():
        # print(position)
        pos_dt = datetime.strptime(position['open_datetime'], "%Y-%m-%d %H:%M")
        # pos_dt = position['open_datetime']
        try:
            if positions_dict.get(pos_dt) is None:
                positions_dict[pos_dt] = [position]
                #     "contracts_purchased": [f"{purchase['option_symbol']}_{purchase['quantity']}"],
                #     "purchase_costs": purchase['contract_cost']*purchase['quantity'],
                # }
            else:
                positions_dict[pos_dt].append(position)
                # purchases_dict[purchase['open_datetime']]['contracts_purchased'].append(f"{purchase['option_symbol']}_{purchase['quantity']}")
                # purchases_dict[purchase['open_datetime']]['purchase_costs'] += (purchase['contract_cost']*purchase['quantity'])
        except Exception as e:
            print(e)
            print(position)
            continue
    return portfolio_dict, positions_dict, passed_trades_dict 

def build_results_df(purchases_list, sales_list, datetime_list):
    portfolio_df = pd.DataFrame(columns=['datetime', 'value'])
    portfolio_value = 100000
    full_results_dict = {}
    for date in datetime_list:
        date_str = date.strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        full_results_dict[dt] = {
            "contracts_purchased": [],
            "purchase_costs": 0,
            "contracts_sold": [],
            "sale_returns": 0,
        }
    for entry in purchases_list:
        try:
            if full_results_dict.get(entry['open_datetime']) is None:
                full_results_dict[entry['open_datetime']] = {
                    "contracts_purchased": [f"{entry['option_symbol']}_{entry['quantity']}"],
                    "purchase_costs": entry['contract_cost']*entry['quantity'],
                    "contracts_sold": [],
                    "sale_returns": 0,
                }
            else:
                full_results_dict[entry['open_datetime']]['contracts_purchased'].append(f"{entry['option_symbol']}_{entry['quantity']}")
                full_results_dict[entry['open_datetime']]['purchase_costs'] += (entry['contract_cost']*entry['quantity'])
        except Exception as e:
            print(e)
            print(entry)
            continue

    for entry in sales_list:
        try:
            if full_results_dict.get(entry['close_datetime']) is None:
                full_results_dict[entry['close_datetime']] = {
                    "contracts_sold": [f"{entry['option_symbol']}_{entry['quantity']}"],
                    "sale_returns": entry['contract_cost']*entry['quantity'],
                    "contracts_purchased": [],
                    "purchase_costs": 0,
                }
            else:   
                full_results_dict[entry['close_datetime']]['contracts_sold'].append(f"{entry['option_symbol']}_{entry['quantity']}")
                full_results_dict[entry['close_datetime']]['sale_returns'] += (entry['contract_cost']*entry['quantity'])
        except Exception as e:
            print(e)
            print(entry)
            continue

    results_df = pd.DataFrame.from_dict(full_results_dict, orient='index')
    results_df = results_df.sort_index(ascending=True)

    cash_values = []
    # holdings_values = []
    # total_portfolio_values = []
    active_holdings = []
    starting_portfolio_value = 100000

    i = 0
    for index, row in results_df.iterrows():
        if len(active_holdings) == 0:
            temp_active = row['contracts_purchased']
            temp_cash = (starting_portfolio_value - row['purchase_costs'])
            # temp_holdings = row['purchase_costs']
            # temp_total_values = temp_cash + temp_holdings
            cash_values.append(temp_cash)
            active_holdings.append(temp_active)
            # holdings_values.append(temp_holdings)
            # total_portfolio_values.append(temp_total_values)
            i += 1
        else:
            temp_active = active_holdings[(i-1)]
            starting_cash = cash_values[(i-1)]
            temp_active = [x for x in temp_active if x not in row['contracts_sold']]
            temp_active.extend(row['contracts_purchased'])
            # temp_cash = (temp_total_values - row['purchase_costs'])
            # temp_holdings = row['purchase_costs']
            # temp_total_values = temp_cash + temp_holdings
            temp_cash = (row['sale_returns'] + starting_cash) - row['purchase_costs']
            cash_values.append(temp_cash)
            active_holdings.append(temp_active)
            # holdings_values.append(temp_holdings)
            # total_portfolio_values.append(temp_total_values)
            i += 1

    results_df['portfolio_cash'] = cash_values
    results_df['active_holdings'] = active_holdings
    results_df['period_net_value'] = results_df['sale_returns'] - results_df['purchase_costs']

                    

    return results_df

def build_positions_df(positions_list):
    positions_dict = {}
    for positions in positions_list:
        pct_returns = []
        gross_returns = []
        for position_id, trades in positions.items():
            position_underlying = position_id.split('-')[0]
            strategy = position_id.split('-')[1]
            for trade in trades:
                pct_returns.append(trade['pct_gain'])
                gross_returns.append(trade['total_gain'])
        if len(pct_returns) == 0:
            print("No trades for this position")
            print(position_id)
            continue
        else:
            positions_dict[position_id] = {
                "pct_returns": sum(pct_returns)/len(pct_returns),
                "gross_returns": sum(gross_returns),
                "underlying_symbol": position_underlying,
                "strategy": strategy,
                "open_datetime": trades[0]['open_trade_dt'],
                "close_datetime": trades[-1]['close_trade_dt'],
            }
    positions_df = pd.DataFrame.from_dict(positions_dict, orient='index')
    return positions_df, positions_dict

def extract_results_dict(positions_list):
    results_dicts = []
    transactions = positions_list['transactions']
    for transaction in transactions:
        try:
            sell_dict = transaction['sell_info']
            buy_dict = transaction['buy_info']
            results_dicts.append(
            {
                "price_change": transaction['price_change'], "pct_gain": transaction['pct_gain'],
                "total_gain": transaction['total_gain'], "open_trade_dt": transaction['open_trade_dt'], 
                "close_trade_dt": transaction['close_trade_dt'],"max_gain_before": sell_dict['max_value_before_pct_change'],
                "max_gain_after": sell_dict['max_value_after_pct_change'],"option_symbol": sell_dict['option_symbol'],
                "max_value_before_date": sell_dict['max_value_before_date'], "max_value_after_date": sell_dict['max_value_after_date'],
                "max_value_before_idx": sell_dict['max_value_before_idx'], "max_value_after_idx": sell_dict['max_value_after_idx'],
                "sell_code": sell_dict['sell_code'], "quantity": buy_dict['quantity'],
            })
        except Exception as e:
            print(f"Error: {e} in extracting results dict")
            print(transaction)
            continue
    return results_dicts

def extract_results_dict_pt(positions_list, config):
    results_dicts = []
    transactions = positions_list['transactions']
    for transaction in transactions:
        try:
            sell_dict = transaction['sell_info']
            buy_dict = transaction['buy_info']
            results_dicts.append(
            {
                "price_change": transaction['price_change'], "pct_gain": transaction['pct_gain'],
                "total_gain": transaction['total_gain'], "open_trade_dt": transaction['open_trade_dt'], 
                "close_trade_dt": transaction['close_trade_dt'],"max_gain_before": sell_dict['max_value_before_pct_change'],
                "max_gain_after": sell_dict['max_value_after_pct_change'],"option_symbol": sell_dict['option_symbol'],
                "max_value_before_date": sell_dict['max_value_before_date'], "max_value_after_date": sell_dict['max_value_after_date'],
                "max_value_before_idx": sell_dict['max_value_before_idx'], "max_value_after_idx": sell_dict['max_value_after_idx'],
                "sell_code": sell_dict['sell_code'], "quantity": buy_dict['quantity'],
            })
        except Exception as e:
            print(f"Error: {e} in extracting results dict")
            print(transaction)
            continue
    return results_dicts

# def extract_results_dict_RMF(positions_list):
#     results_dicts = []
#     transactions = positions_list['transactions']
#     for transaction in transactions:
#         sell_dict = transaction['sell_info']
#         results_dicts.append(
#         {
#             "price_change": transaction['price_change'], "pct_gain": transaction['pct_gain'],
#             "total_gain": transaction['total_gain'], "open_trade_dt": transaction['open_trade_dt'], 
#             "close_trade_dt": transaction['close_trade_dt'],"max_gain_before": sell_dict['max_value_before_pct_change'],
#             "max_gain_after": sell_dict['max_value_after_pct_change'],"option_symbol": sell_dict['option_symbol'],
#             "max_value_before_date": sell_dict['max_value_before_date'], "max_value_after_date": sell_dict['max_value_after_date'],
#             "max_value_before_idx": sell_dict['max_value_before_idx'], "max_value_after_idx": sell_dict['max_value_after_idx'],
#             "sell_code": sell_dict['sell_code'], "aggregate_classification": sell_dict['aggregate_classification'],
#             "hilo_score": sell_dict['hilo_score']
#         })
#     return results_dicts


def create_datetime_index(start_date, end_date):
    print("DATE TIME INDEX")
    print(start_date)
    print(end_date)
    print()
    datetime_index = pd.date_range(start_date, end_date, freq=f"15min", name = 'Time')
    days = []
    for time in datetime_index:
        convertedtime = time.strftime('%Y-%m-%d %H:%M:%S')
        finaldate = datetime.strptime(convertedtime, '%Y-%m-%d %H:%M:%S')
        days.append(finaldate)
    results = pd.DataFrame(index=days)
    return days, datetime_index, results

    
def create_portfolio_date_list(start_date, end_date, config):
    sy, sm, sd = start_date.split('/')
    ey, em, ed = end_date.split('/')
    start_time = datetime(int(sy), int(sm), int(sd), 9, 30)
    end_time = datetime(int(ey), int(em), int(ed), 16, 0)
    end_date = create_end_date(end_time, 4)
    date_list, _, _  = create_datetime_index(start_time, end_date)
    return date_list

def map_assignment_adjustment(aa):
    if aa == 1:
        return 3,4


def build_put_list(opt_chain, expiry, symbol):
    df = opt_chain.sort_values('strike', ascending=False)
    df['contract_symbol'] = df['contractSymbol'].apply(lambda x: f"{symbol}{expiry.strftime('%y%m%d')}{x.split('P')[1]}")
    return df['contract_symbol'].tolist()

def build_call_list(opt_chain, expiry, symbol):
    df = opt_chain.sort_values('strike', ascending=False)
    df['contract_symbol'] = df['contractSymbol'].apply(lambda x: f"{symbol}{expiry.strftime('%y%m%d')}{x.split('C')[1]}")
    return df['contract_symbol'].tolist()

def get_current_2wk():
    # Get the next Friday from the start date
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    noww = datetime(year, month, day)
    current_date = noww + timedelta(days=(4 - noww.weekday() + 7) % 7)

    # Get the Friday after next
    current_2wk = current_date + timedelta(weeks=2)

    return current_2wk

def get_friday_after_next(start_date):
    # Get the next Friday from the start date
    current_date = start_date + timedelta(days=(4 - start_date.weekday() + 7) % 7)

    # Get the Friday after next
    friday_after_next = current_date + timedelta(weeks=2)

    return friday_after_next

def approve_trade(portfolio_cash, threshold_cash, position_id, current_positions):
    if portfolio_cash > threshold_cash:
        print("Not enough cash")
        return False
    
def approve_trade_poslimit(portfolio_cash, threshold_cash, position_id, current_positions):
    if portfolio_cash > threshold_cash:
        if position_id not in current_positions:
            return True
        else:
            print(f"Position {position_id} is already taken")
            return False
    else:
        print("Not enough cash")
        return False
    
def build_options_df(contracts, row):
    if row['symbol'] in ["GOOG","GOOGL","NVDA","AMZN","TSLA"]:
        last_price = ph.get_last_price(row)
        row['alert_price'] = last_price
    df = pd.DataFrame(contracts, columns=['contract_symbol'])
    df['underlying_symbol'] = row['symbol']
    df['option_type'] = row['side']
    try:
        df['strike'] = df.apply(lambda x: extract_strike(x),axis=1)
    except Exception as e:
        print(f"Error: {e} building options df for {row['symbol']}")
        print(df)
        print(contracts)
        return df

    df['strike_diff'] = abs((df['strike'] - row['alert_price'])/row['alert_price'])
    if row['side'] == "P":
        df['in_money'] = df['strike']  > row['alert_price']
        df_in = df.loc[df['strike'] > row['alert_price']]
        df_out = df.loc[df['strike'] < row['alert_price']]
        last_in = df_in.head(1)
        # Concatenate the True rows and the last False row
        result = pd.concat([last_in, df_out])
        result = result.sort_values('strike', ascending=False)
        # df  = df.loc[df['strike_diff'] < 0.075].reset_index(drop=True)
        # print(df)
        # breakkk
    elif row['side'] == "C":
        df['in_money'] = df['strike']  < row['alert_price']
        # Select all rows where 'condition' is True
        df_in = df.loc[df['strike'] < row['alert_price']]
        df_out = df.loc[df['strike'] > row['alert_price']]
        last_in = df_in.tail(1)
        # Concatenate the True rows and the last False row
        result = pd.concat([last_in, df_out])
        result = result.sort_values('strike', ascending=True)

    
    return result



def extract_strike(row):
    str = row['contract_symbol'].split(f"O:{row['underlying_symbol']}")[1]
    if row['option_type'] == 'P':
        str = str.split('P')[1]
    elif row['option_type'] == 'C':
        str = str.split('C')[1]
    strike = str[:-3]
    for i in range(len(strike)):
        if strike[i] == '0':
            continue
        else:
            return int(strike[i:])
        
    return 0

# def configure_regression_predictions(backtest_data, config):
#     backtest_data = backtest_data[backtest_data['threeD_stddev50'] > 0]
#     forecast_vols = []
#     for index, row in backtest_data.iterrows():
#         if row['strategy'] in ['BFC','BFP']:
#             forecast_vols.append(abs(row['forecast']/row['threeD_stddev50']))
#         else:
#             forecast_vols.append(abs(row['forecast']/row['oneD_stddev50']))
#     backtest_data['forecast_vol'] = forecast_vols
#     # backtest_data['forecast_vol'] = backtest_data.apply(lambda x: x['forecast']/x['threeD_stddev50'] if x['strategy'in ['BFC','BFP']] else x['forecast']/x['oneD_stddev50'],axis=1)
#     data = backtest_data.loc[backtest_data['forecast_vol'] > config['volatility_threshold']].reset_index(drop=True)
#     return data

def configure_trade_data(df,config):
    # index = df.loc[df['symbol'].isin(["IWM","SPY","QQQ"])]
    # stocks = df.loc[df['symbol'].isin(["IWM","SPY","QQQ"]) == False]


    one = df.loc[df['prediction_horizon'] == "1"]
    # one_idx = index.loc[index['prediction_horizon'] == "1"]
    # three = stocks.loc[stocks['prediction_horizon'] == "3"]
    # one_idx = index.loc[index['prediction_horizon'] == "1"]
    # three_idx = index.loc[index['prediction_horizon'] == "3"]

    if config['days'] == '23':
        filt_one = one.loc[one['day_of_week'].isin([2,3])]
    elif config['days'] == '123':
        filt_one = one.loc[one['day_of_week'].isin([1,2,3])]
    elif config['days'] == '12':
        filt_one = one.loc[one['day_of_week'].isin([1,2])]
    else:
        filt_one = one.loc[one['day_of_week'].isin([0,1,2,3])]

    filt_one = filt_one.loc[filt_one['symbol'] != 'GOOG']
    return filt_one
    # if config['IDX'] == "ONLY":
    #     return one_idx
    # elif config['IDX'] ==  False:
    #     one_idx = one_idx.loc[one['day_of_week'].isin([2,3])]
    #     trades = pd.concat([filt_one,one_idx])
    #     return trades
    # elif config['IDX'] == True:
    #     trades = pd.concat([filt_one,one_idx])
    #     return trades

    # filt_three = three.loc[three['day_of_week'].isin([0,1,2])]

    # one_idxF = one_idx.loc[one_idx['day_of_week'].isin([0,1,2,3])]
    # three_idxF = three_idx.loc[three_idx['day_of_week'].isin([0,1,2])]

    # trade_df = pd.concat([filt_one,filt_three,one_idxF,three_idxF])
    # trade_df = pd.concat([stocks,index])
    # trade_df = pd.concat([filt_one,one_idx])


