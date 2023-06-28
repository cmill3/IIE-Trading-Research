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

def create_end_date(start_date, add_days):
    #Trading days only
    trading_days_to_add = add_days
    while trading_days_to_add > 0:
        start_date += timedelta(days=1)
        weekday = start_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    
    return start_date

def s3_data(s3link):
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    bucket_name = s3link['bucketname']
    object_key = s3link['objectkey']
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
    i = 1
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

def generate_datetime_range(start_date, end_date):
    delta = timedelta(minutes=15)
    current_date = start_date
    datetime_range = []

    while current_date <= end_date:
        datetime_range.append(current_date.strftime("%Y-%m-%dT%H:%M"))
        current_date += delta

    return datetime_range


def build_results_df(purchases_list, sales_list, datetime_list):
    """
        This current idea is not working. Current plan is to create a master dict, with each datetime that is possible as the keys
        and the values can then be derived by looping through all of the transactions. Those can then be returned into the template 
        from which the final df can be constructed.
    """
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
            }
    positions_df = pd.DataFrame.from_dict(positions_dict, orient='index')
    return positions_df

def polygon_optiondata(options_ticker, from_date, to_date):
    #This is for option data
    multiplier = "15"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)
    timespan = "minute"
    # from_stamp = standardize_dates(from_stamp)
    # to_stamp = standardize_dates(to_stamp)
    url = f"https://api.polygon.io/v2/aggs/ticker/{options_ticker}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    res_option_df = pd.DataFrame(response_data['results'])
    res_option_df['t'] = res_option_df['t'].apply(lambda x: int(x/1000))
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

def build_table(start_date, end_date):
    datetimeindex = pd.date_range(start_date, end_date, freq='15min', name = 'Time')
    days = []
    for time in datetimeindex:
        convertedtime = time.strftime('%Y-%m-%d %H:%M:%S')
        finaldate = datetime.strptime(convertedtime, '%Y-%m-%d %H:%M:%S')
        days.append(finaldate)
    results = pd.DataFrame(index=days)
    return days, datetimeindex, results

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
    