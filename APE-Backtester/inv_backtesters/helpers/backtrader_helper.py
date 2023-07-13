from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json
import ast
import holidays
import warnings
import helpers.trading_strategies as ts
from yahooquery import Ticker
# from pandas._libs.mode_warnings import SettingWithCopyWarning


warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


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
    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
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

def s3_data(bucket_name, object_key):
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    rawdata = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(rawdata))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    df['contracts'] = df['contracts'].apply(lambda x: ast.literal_eval(x))
    df['contracts_available'] = df['contracts'].apply(lambda x: len(x)>=12)
    return df

def s3_data_inv(bucket_name, object_key, prefixes):
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    rawdata = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(rawdata))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    df['contracts'] = df['contracts'].apply(lambda x: ast.literal_eval(x))
    df['contracts_available'] = df['contracts'].apply(lambda x: len(x)>=12)
    return df

def create_results_dict(buy_dict, sell_dict):
    price_change = sell_dict['close_price'] - buy_dict['open_price']
    pct_gain = price_change / buy_dict['open_price']
    total_gain = (price_change*100) * buy_dict['quantity']
    results_dict = {
                    "price_change":price_change, "pct_gain":pct_gain, "total_gain":total_gain, 
                    "open_trade_dt": buy_dict['open_datetime'], "option_contract": buy_dict['option_symbol'],
                    "close_trade_dt": sell_dict['close_datetime'],
                    }
    return results_dict

def create_options_aggs(symbol, start_date, end_date,price, direction, contracts, spread_length):
    options = []
    ticker_data = []
    t_2wk = timedelta((11 - start_date.weekday()) % 14)
    expiry = (start_date + t_2wk).strftime("%y%m%d")
    strike = symbol + expiry
    filtered_contracts = [k for k in contracts if strike in k]
    for contract in filtered_contracts:
        try:
            df_tickerdata = polygon_optiondata(contract, start_date, end_date)
            trading_df = polygon_stockdata(symbol,start_date, end_date, df_tickerdata)
            ticker_data.append(df_tickerdata)
            options.append(contract)
            if len(options) >= spread_length:
                break
        except:
            continue
    return ticker_data, options

def create_options_aggs_inv(symbol, contracts, start_date, end_date, market_price, strategy, spread_length):
    print("Creating options aggregates...")
    options = []
    enriched_options_aggregates = []
    t_2wk = timedelta((11 - start_date.weekday()) % 14)
    expiry = (start_date + t_2wk).strftime("%y%m%d")
    strike = symbol + expiry

    # contracts = build_contracts(symbol, start_date, end_date, market_price, strategy, spread_length)
    underlying_agg_data = polygon_stockdata_inv(symbol,start_date,end_date)
    contracts = ast.literal_eval(contracts)
    filtered_contracts = [k for k in contracts if strike in k]
    for contract in filtered_contracts:
        try:
            options_agg_data = polygon_optiondata(contract, start_date, end_date)
            underlying_price = []
            for i, row in options_agg_data.iterrows():
                matched_row = underlying_agg_data.loc[underlying_agg_data['date'] == row['date']]
                open_price = matched_row.o.values
                underlying_price.append(str(round(open_price[0],2)))
            options_agg_data['underlying_price'] = underlying_price
            enriched_options_aggregates.append(options_agg_data)
            options.append(contract)
            if len(options) >= spread_length:
                break
        except Exception as e:
            print(contract)
            print(strategy)
            print(e)
            continue
    return enriched_options_aggregates, options

def generate_datetime_range(start_date, end_date):
    delta = timedelta(minutes=15)
    current_date = start_date
    datetime_range = []

    while current_date <= end_date:
        print("2")
        datetime_range.append(current_date.strftime("%Y-%m-%dT%H:%M"))
        current_date += delta

    return datetime_range

def convert_lists_to_dicts(positions_list, datetime_list):
    portfolio_dict = {}
    positions_dict = {}
    sales_dict = {}
    passed_trades_dict = {}

    for date in datetime_list:
        # date_str = date.strftime("%Y-%m-%d %H:%M:%S")
        # dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        portfolio_dict[date] = {
            "contracts_purchased": [],
            "purchase_costs": 0,
            "contracts_sold": [],
            "sale_returns": 0,
            "portfolio_cash": 0,
            "active_holdings": [],
            "period_net_returns": 0,
            "open_positions": [],
        }

    for position in positions_list:
        print(type(position['open_datetime']))
        pos_dt = datetime.strptime(position['open_datetime'], "%Y-%m-%d %H:%M:%S")
        try:
            if positions_dict.get(position['open_datetime']) is None:
                positions_dict[position['open_datetime']] = [position]
                #     "contracts_purchased": [f"{purchase['option_symbol']}_{purchase['quantity']}"],
                #     "purchase_costs": purchase['contract_cost']*purchase['quantity'],
                # }
            else:
                positions_dict[position['open_datetime']].append(position)
                # purchases_dict[purchase['open_datetime']]['contracts_purchased'].append(f"{purchase['option_symbol']}_{purchase['quantity']}")
                # purchases_dict[purchase['open_datetime']]['purchase_costs'] += (purchase['contract_cost']*purchase['quantity'])
        except Exception as e:
            print(e)
            print(position)
            continue

def convert_lists_to_dicts_inv(positions_list, datetime_list):
    portfolio_dict = {}
    positions_dict = {}
    sales_dict = {}
    passed_trades_dict = {}

    for date in datetime_list:
        # date_str = date.strftime("%Y-%m-%d %H:%M:%S")
        # dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        portfolio_dict[date] = {
            "contracts_purchased": [],
            "purchase_costs": 0,
            "contracts_sold": [],
            "sale_returns": 0,
            "portfolio_cash": 0,
            "active_holdings": [],
            "period_net_returns": 0,
            "open_positions": [],
        }

    for position in positions_list:
        pos_dt = datetime.strptime(position['open_datetime'], "%Y-%m-%d %H:%M:%S")
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

    
    # for purchase in purchases_list:
    #     try:
    #         if purchases_dict.get(purchase['open_datetime']) is None:
    #             purchases_dict[purchase['open_datetime']] = [purchase]
    #             #     "contracts_purchased": [f"{purchase['option_symbol']}_{purchase['quantity']}"],
    #             #     "purchase_costs": purchase['contract_cost']*purchase['quantity'],
    #             # }
    #         else:
    #             purchases_dict[purchase['open_datetime']].append(purchase)
    #             # purchases_dict[purchase['open_datetime']]['contracts_purchased'].append(f"{purchase['option_symbol']}_{purchase['quantity']}")
    #             # purchases_dict[purchase['open_datetime']]['purchase_costs'] += (purchase['contract_cost']*purchase['quantity'])
    #     except Exception as e:
    #         print(e)
    #         print(purchase)
    #         continue

    # for sale in sales_list:
    #     try:
    #         if sales_dict.get(sale['close_datetime']) is None:
    #             sales_dict[sale['close_datetime']] = [sale]
    #             #     "contracts_sold": [f"{sale['option_symbol']}_{sale['quantity']}"],
    #             #     "sale_returns": (sale['contract_cost']*sale['quantity']),
    #             # }
    #         else:
    #             sales_dict[sale['close_datetime']].append(sale)
    #             # sales_dict[sale['open_datetime']]['contracts_sold'].append(f"{sale['option_symbol']}_{sale['quantity']}")
    #             # sales_dict[sale['open_datetime']]['sale_returns'] += (sale['contract_cost']*sale['quantity'])
    #     except Exception as e:
    #         print(e)
    #         print(sale)
    #         continue
    return portfolio_dict, positions_dict, passed_trades_dict

def simulate_portfolio(positions_list, datetime_list, portfolio_cash):
    positions_taken = []
    contracts_bought = []
    contracts_sold = []
    # total_holdings = []
    total_positions = []
    starting_cash = portfolio_cash
    # starting_date = datetime_list[0]
    # print(starting_date)
    purchases_dict = {}
    sales_dict = {}
    portfolio_dict, positions_dict, passed_trades_dict = convert_lists_to_dicts_inv(positions_list, datetime_list)

    ## What we need is to at this point build the trade. We need to send through the package of contracts in 
    ## their bundle of a "position", then we can approximate bet sizing and the contract sizing at this point in time.
    ## Then from there we can build the results of the trade, and then we can build the portfolio from there.
    ## This will give us dynamic and rective sizing.
    
    i = 0
    for key, value in portfolio_dict.items():
        current_positions = []
        # current_holdings = []
        if i == 0:
            value['portfolio_cash'] = portfolio_cash
            # current_holdings = []

            if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if value['portfolio_cash'] > (0.5 * starting_cash):
                        sized_buys, sized_sells = ts.build_trade(position, value['portfolio_cash'])
                        for index, order in enumerate(sized_buys):
                            if order != None:
                                value['contracts_purchased'].append(f"{order['option_symbol']}_{order['order_id']}")
                                value['purchase_costs'] += (order['contract_cost'] * order['quantity'])
                                value['portfolio_cash'] -= order['contract_cost'] * order['quantity']
                                contracts_bought.append(f"{order['option_symbol']}_{order['order_id']}")
                                sale_values = sized_sells[index]
                                if sales_dict.get(sized_sells[index]['close_datetime']) is None:
                                    sales_dict[sized_sells[index]['close_datetime']] = [sized_sells[index]]
                                else:
                                    sales_dict[sized_sells[index]['close_datetime']].append(sized_sells[index])
                        current_positions.append((position['position_id'].split("-")[0] + position['position_id'].split("-")[1]))
                        positions_taken.append(position)
                        value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
                        # if purchase['position_id'] not in value['open_positions']:
                        #     value['open_positions'].append(purchase['position_id'])
                        #     current_positions.append(purchase['position_id'])
                        #     positions_taken.append(purchase['position_id'])
                    else:
                        if passed_trades_dict.get(key) is not None:
                            passed_trades_dict[key]['trades'].append(position)
                        else:
                            passed_trades_dict[key] = {
                                "trades": [position]
                            }
                i += 1
                value['open_positions'].extend(current_positions)
                # value['active_holdings'].extend(contracts_bought)
                # total_positions.append(current_positions)
                continue
        else:
            value['portfolio_cash'] = portfolio_dict[key - timedelta(minutes=15)]['portfolio_cash']
            # current_holdings = portfolio_dict[key - timedelta(minutes=15)]['active_holdings']
            # current_positions = total_positions[-1]
            current_positions = portfolio_dict[key - timedelta(minutes=15)]['open_positions']

            # open_positions = portfolio_dict[key - timedelta(minutes=15)]['open_positions']
        
        if sales_dict.get(key) is not None:
            for sale in sales_dict[key]:
                if (f"{sale['option_symbol']}_{sale['order_id']}") in contracts_bought:
                    value['contracts_sold'].append(f"{sale['option_symbol']}_{sale['order_id']}")
                    value['sale_returns'] += (sale['contract_cost'] * sale['quantity'])
                    # current_holdings.remove(f"{sale['option_symbol']}_{sale['quantity']}")
                    value['portfolio_cash'] += (sale['contract_cost'] * sale['quantity'])
                    contracts_sold.append(f"{sale['option_symbol']}_{sale['order_id']}")
                    if (sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]) in current_positions:
                        current_positions.remove((sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]))    

        if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if approve_trade(value['portfolio_cash'],(.5 * starting_cash),position['position_id'].split("-")[0] + position['position_id'].split("-")[1], current_positions):
                        sized_buys, sized_sells = ts.build_trade(position, value['portfolio_cash'])
                        if sized_buys == None:
                            print(position)
                            continue
                        for index, order in enumerate(sized_buys):
                            if order == None:
                                print(sized_buys)
                                print(sized_sells)
                                continue
                            else:
                                value['contracts_purchased'].append(f"{order['option_symbol']}_{order['order_id']}")
                                value['purchase_costs'] += (order['contract_cost'] * order['quantity'])
                                value['portfolio_cash'] -= (order['contract_cost'] * order['quantity'])
                                contracts_bought.append(f"{order['option_symbol']}_{order['order_id']}")
                                # current_holdings.append(f"{order['option_symbol']}_{order['order_id']}")
                                sale_values = sized_sells[index]
                                ## How do we integrate this with sales at a later date?
                                if sales_dict.get(sized_sells[index]['close_datetime']) is None:
                                    sales_dict[sized_sells[index]['close_datetime']] = [sized_sells[index]]
                                else:
                                    sales_dict[sized_sells[index]['close_datetime']].append(sized_sells[index])
                            if (position['position_id'].split("-")[0] + position['position_id'].split("-")[1]) not in current_positions:
                                current_positions.append((position['position_id'].split("-")[0] + position['position_id'].split("-")[1]))
                            if position not in positions_taken:
                                positions_taken.append(position)
                else:
                    if passed_trades_dict.get(key) is not None:
                        passed_trades_dict[key]['trades'].append(position)
                    else:
                        passed_trades_dict[key] = {
                            "trades": [position]
                        }
        # value['active_holdings'] = current_holdings
        # total_holdings.append(current_positions)
        
        value['open_positions'].extend(current_positions)
        # value['active_holdings'].extend(current_holdings)
        value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
   
        # except Exception as e:
        #     print(e)
        #     print(key)
        #     continue
    portfolio_df = pd.DataFrame.from_dict(portfolio_dict, orient='index')
    # portfolio_df['open_positions'] = total_holdings
    passed_trades_df = pd.DataFrame.from_dict(passed_trades_dict, orient='index')
    print("Elements in bought but not in sold:")
    diff = list(set(contracts_bought) - set(contracts_sold))
    print(diff)
    print("Elements in sold but not in bought:")
    diff2 = list(set(contracts_sold) - set(contracts_bought))
    print(diff2)
    return portfolio_df, passed_trades_df, positions_taken, positions_dict 

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

def polygon_optiondata(options_ticker, from_date, to_date):
    #This is for option data
    multiplier = "15"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    print(from_date)
    print(to_date)
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)
    timespan = "minute"
    # from_stamp = standardize_dates(from_stamp)
    # to_stamp = standardize_dates(to_stamp)
    url = f"https://api.polygon.io/v2/aggs/ticker/O:{options_ticker}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    res_option_df = pd.DataFrame(response_data['results'])
    res_option_df['t'] = res_option_df['t'].apply(lambda x: int(x/1000))
    res_option_df['t']  = res_option_df['t'] + timedelta(hours=4).total_seconds()
    res_option_df['date'] = res_option_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    res_option_df['ticker'] = options_ticker
    #res_option_df.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-BAcktester/APE-Backtester-Results/Testing_Research_Data_CSV_{x}_{from_date}.csv')
    return res_option_df

def polygon_stockdata(x, from_date, to_date, df_optiondata):
    #This is for underlying data
    multiplier = "15"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload = {}
    headers = {}
    stocksTicker = x
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)
    timespan = "minute"
    # from_stamp = standardize_dates(from_stamp)
    # to_stamp = standardize_dates(to_stamp)
    url = f"https://api.polygon.io/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['t']  = stock_df['t'] + timedelta(hours=4).total_seconds()
    stock_df['date'] = stock_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    underlying_price = []
    for i, row in df_optiondata.iterrows():
        matched_row = stock_df.loc[stock_df['date'] == row['date']]
        open_price = matched_row.o.values
        underlying_price.append(str(round(open_price[0],2)))
    df_optiondata['underlying_price'] = underlying_price
    new_date = from_date.strftime("D:" + "%Y%m%d" + "T:" + "%H-%M-%S")
    # df_optiondata.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_{x}_{new_date}.csv')
    return df_optiondata

def polygon_stockdata_inv(symbol, from_date, to_date):
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/15/minute/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit=50000&apiKey={key}"

    response = requests.request("GET", url, headers={}, data={})
    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['t']  = stock_df['t'] + timedelta(hours=4).total_seconds()
    stock_df['date'] = stock_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    return stock_df

def data_pull(symbol, start_date, end_date, mktprice, strategy, contracts):
    trading_aggregates = create_option(symbol, start_date, end_date, mktprice, strategy, contracts, spread_length=3)
    return trading_aggregates

def set_data(from_date, to_date):
    data = bt.feeds.GenericCSVData(
        dataname= f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_RBLX_2023-01-02 14/00/00.csv',
        fromdate=from_date,
        todate=to_date,
        dtformat=('%Y-%m-%d %H:%M:%S'),
        datetime=9,
        open = 3,
        high = 5,
        low = 6,
        close = 4,
        volume =1, 
        openinterest=-1,
        reverse=False)
    return data

def convertepoch(time):
    epochtime = time.astype(datetime)
    closetime = int(epochtime) / 1000000000
    closedate = datetime.utcfromtimestamp(closetime).strftime('%Y-%m-%d %H:%M:%S')
    finaldate = datetime.strptime(closedate, '%Y-%m-%d %H:%M:%S')
    return finaldate

def create_datetime_index(start_date, end_date):
    datetime_index = pd.date_range(start_date, end_date, freq='15min', name = 'Time')
    days = []
    for time in datetime_index:
        convertedtime = time.strftime('%Y-%m-%d %H:%M:%S')
        finaldate = datetime.strptime(convertedtime, '%Y-%m-%d %H:%M:%S')
        days.append(finaldate)
    results = pd.DataFrame(index=days)
    return days, datetime_index, results

def build_dict(seq, key):
    return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))

def tradingdaterange(start_date, end_date):
    alldates = []
    operating_dates = []
    start = datetime.strptime(start_date, "%Y/%m/%d")
    end = datetime.strptime(end_date, "%Y/%m/%d")
    for n in range(int ((end - start).days)+1):
            alldates.append(start + timedelta(n))

    nyse_holidays = holidays.NYSE()
    
    weekdays = [5,6]

    for i in alldates:
        if i.weekday() not in weekdays and i not in nyse_holidays:                    # to print only the weekdates
            operating_dates.append(i.strftime('%Y/%m/%d'))

    return operating_dates

def pull_modeling_results(alerts_df):
    alerts_df['date_str'] = alerts_df['date'].apply(lambda x: x.split(' ')[0].replace('-','/'))
    alerts_df['hour'] = alerts_df['date'].apply(lambda x: x.split(' ')[1][0:2])
    alerts_df['date'] = pd.to_datetime(alerts_df['date'])

    hours = ["14","15","16","17","18","19"]
    date_str= alerts_df['date_str'].unique()
    model_results = []
    for hour in hours:
        try:
            print(f"yqalerts_full_results/{date_str[0]}/{hour}.csv")
            dataset = s3.get_object(Bucket="yqalerts-model-results", Key=f"yqalerts_full_results/{date_str[0]}/{hour}:00.csv")
            df = pd.read_csv(dataset.get("Body"))
            df['hour'] = hour
            model_results.append(df)
        except Exception as e:
            print(e)
            print(f"yqalerts_full_results/{date_str[0]}/{hour}.csv")
            continue
    model_results = pd.concat(model_results)
    model_results = model_results.rename(columns={'strategy': 'title'})
    merged_df = pd.merge(alerts_df, model_results, on=['symbol', 'hour','title'], how='inner')
    merged_df.drop(["Unnamed: 0_x","Unnamed: 0_y","Unnamed: 0.1"], axis=1, inplace=True)
    return merged_df

# def create_full_date_list(start_date, end_date):
#     start_time = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
#     end_date = create_end_date(start_time, 4)
#     datetime_list = create_datetime_index(start_time, end_date)
#     return datetime_list
    
def create_portfolio_date_list(start_date, end_date):
    start_time = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    end_date = create_end_date(end_date, 3)
    date_list, _, _  = create_datetime_index(start_time, end_date)
    return date_list




# def simulate_portfolio(positions_list, datetime_list, portfolio_cash):
#     positions_taken = []
#     contracts_bought = []
#     contracts_sold = []
#     starting_cash = portfolio_cash
#     # starting_date = datetime_list[0]
#     # print(starting_date)
#     portfolio_dict, positions_dict, passed_trades_dict = convert_lists_to_dicts(positions_list, datetime_list)

#     ## What we need is to at this point build the trade. We need to send through the package of contracts in 
#     ## their bundle of a "position", then we can approximate bet sizing and the contract sizing at this point in time.
#     ## Then from there we can build the results of the trade, and then we can build the portfolio from there.
#     ## This will give us dynamic and rective sizing.
    
#     i = 0
#     for key, value in portfolio_dict.items():
#         if i == 0:
#             value['portfolio_cash'] = portfolio_cash
#             current_positions = []
#             # current_holdings = []

#             if positions_dict.get(key) is not None:
#                 for purchase in positions_dict[key]:
#                     if value['portfolio_cash'] > (0.5 * starting_cash):
#                         contracts, quantity = ts.bet_sizer(contracts, value['portfolio_cash'])
#                         value['contracts_purchased'].append(f"{purchase['option_symbol']}_{purchase['order_id']}")
#                         value['purchase_costs'] += purchase['contract_cost']
#                         # current_holdings.append(f"{purchase['option_symbol']}_{purchase['quantity']}")
#                         value['portfolio_cash'] -= purchase['contract_cost']
#                         contracts_bought.append(f"{purchase['option_symbol']}_{purchase['order_id']}")
#                         if purchase['position_id'] not in value['open_positions']:
#                             value['open_positions'].append(purchase['position_id'])
#                             current_positions.append(purchase['position_id'])
#                             positions_taken.append(purchase['position_id'])
#                     else:
#                         if passed_trades_dict.get(key) is not None:
#                             passed_trades_dict[key]['trades'].append(purchase)
#                         else:
#                             passed_trades_dict[key] = {
#                                 "trades": [purchase]
#                             }
#                 i += 1
#                 continue
#         else:
#             value['portfolio_cash'] = portfolio_dict[key - timedelta(minutes=15)]['portfolio_cash']
#             value['active_holdings'] = portfolio_dict[key - timedelta(minutes=15)]['active_holdings']
#             value['open_positions'] = portfolio_dict[key - timedelta(minutes=15)]['open_positions']
        

#         # try:
#         if sales_dict.get(key) is not None:
#             for sale in sales_dict[key]:
#                 if (f"{sale['option_symbol']}_{sale['order_id']}") in contracts_bought:
#                     value['contracts_sold'].append(f"{sale['option_symbol']}_{sale['order_id']}")
#                     value['sale_returns'] += sale['contract_cost']
#                     # current_holdings.remove(f"{sale['option_symbol']}_{sale['quantity']}")
#                     value['portfolio_cash'] += sale['contract_cost']
#                     contracts_sold.append(f"{sale['option_symbol']}_{sale['order_id']}")
#                     if sale['position_id'] in current_positions:
#                         current_positions.remove(sale['position_id'])    

#         if purchases_dict.get(key) is not None:
#             for purchase in purchases_dict[key]:
#                 if value['portfolio_cash'] > (0.5 * starting_cash):
#                     value['contracts_purchased'].append(f"{purchase['option_symbol']}_{purchase['order_id']}")
#                     value['purchase_costs'] += purchase['contract_cost']
#                     # current_holdings.append(f"{purchase['option_symbol']}_{purchase['quantity']}")
#                     value['portfolio_cash'] -= purchase['contract_cost']
#                     contracts_bought.append(f"{purchase['option_symbol']}_{purchase['order_id']}")
#                     if purchase['position_id'] not in value['open_positions']:
#                         value['open_positions'].append(purchase['position_id'])
#                         current_positions.append(purchase['position_id'])
#                 else:
#                     if passed_trades_dict.get(key) is not None:
#                         passed_trades_dict[key]['trades'].append(purchase)
#                     else:
#                         passed_trades_dict[key] = {
#                             "trades": [purchase]
#                         }
#         # value['active_holdings'] = current_holdings
#         value['current_positions'] = current_positions
        

#         value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
   
#         # except Exception as e:
#         #     print(e)
#         #     print(key)
#         #     continue

#     portfolio_df = pd.DataFrame.from_dict(portfolio_dict, orient='index')
#     passed_trades_df = pd.DataFrame.from_dict(passed_trades_dict, orient='index')
#     print(contracts_bought)
#     print(contracts_sold)
#     diff = list(set(contracts_bought) - set(contracts_sold))
#     print("Elements in bought but not in sold:")
#     print(diff)
#     return portfolio_df, passed_trades_df, positions_taken, portfolio_dict 


def build_contracts(symbol, start_date, end_date, market_price, strategy, spread_length):
    if strategy == 'losers' or strategy == 'gainersP' or strategy == 'vdiffP' or strategy == 'maP':
        option_type = 'puts'
    elif strategy == 'gainers' or strategy == 'vdiffC' or strategy == 'ma' or strategy == 'losersC':
        option_type = 'calls'
    try:
        current_2wk = get_current_2wk()
        expiry_2wk = get_friday_after_next(start_date)
        Tick = Ticker(str(symbol))
        df_ocraw = Tick.option_chain #pulling the data into a data frame (optionchainraw = ocraw)
        df_optionchain_2wk = df_ocraw.loc[symbol, current_2wk, option_type]
        if option_type == 'calls':
            option_chain = df_optionchain_2wk[df_optionchain_2wk['strike'] > market_price]
            contracts = build_call_list(option_chain, expiry_2wk, symbol)
        elif option_type == 'puts':
            option_chain = df_optionchain_2wk[df_optionchain_2wk['strike'] < market_price]
            contracts = build_put_list(option_chain, expiry_2wk, symbol)
    except Exception as e:
        print(e)
        print("Error building contracts")
        contracts = []

    return contracts


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
        if position_id not in current_positions:
            return True
        else:
            print("Position already taken")
            print(position_id)
            return False
    else:
        print("Not enough cash")
        return False