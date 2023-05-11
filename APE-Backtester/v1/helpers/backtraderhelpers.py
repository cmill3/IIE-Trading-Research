from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json
import ast

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

def startbacktrader(Starting_Cash):
    Starting_Value = Starting_Cash
    return Starting_Value

def round_up_to_base(x, base=5):
    return x + (base - x) % base

def round_down_to_base(x, base=5):
    return x - (x % base)

def end_date(start_date, add_days):
    #Trading days only
    trading_days_to_add = add_days
    while trading_days_to_add > 0:
        start_date += timedelta(days=1)
        weekday = start_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    
    return start_date

def s3_data():
    #Pulls training set data from s3
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    bucket_name = 'icarus-research-data'
    object_key = 'training_datasets/expanded_1d_datasets/2023/04/17.csv'
    obj = s3.get_object(Bucket = bucket_name, Key = object_key)
    rawdata = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(rawdata))
    df.dropna(inplace = True)
    df.reset_index(inplace= True, drop = True)
    df['contracts'] = df['contracts'].apply(lambda x: ast.literal_eval(x))
    df['contracts_available'] = df['contracts'].apply(lambda x: len(x)>=12)
    return df

def create_option(symbol, date, price, direction, contracts):

    if direction == "day_gainers" or "most_actives" or "most_watched":
        CallPut = "C"
    elif direction == "day_losers":
        CallPut = "P"
    else:
        CallPut = "C"

    t_2wk = timedelta((11 - date.weekday()) % 14)
    Expiry = (date + t_2wk).strftime("%y%m%d")
    x = str(round(price))
    datadicts = []

    if len(x) == 1:
        y = "0" + x
    else:
        y = x
    
    optionsymbol = "O:" + symbol + Expiry + CallPut + "000" + y + "000"

    for item in contracts:
        if item == optionsymbol:
            return optionsymbol, CallPut
            break
        else:
            continue


    for item in contracts:
        if len(x) == 1:
            strike = item[-4]
            strike_dict = {
                'contract': item,
                'strike': strike
                }
            datadicts.append(strike_dict)
        elif len(x) == 2:
            strike = item[-5:-3]
            strike_dict = {
                'contract': item,
                'strike': strike
                }
            datadicts.append(strike_dict)
        elif len(x) == 3:
            strike = item[-6:-3]
            strike_dict = {
                'contract': item,
                'strike': strike
                }
            datadicts.append(strike_dict)
        elif len(x) == 4:
            strike = item[-7:-3]
            strike_dict = {
                'contract': item,
                'strike': strike
                }
            datadicts.append(strike_dict)
        elif len(x) == 5:
            strike = item[-8:-3]
            strike_dict = {
                'contract': item,
                'strike': strike
                }
            datadicts.append(strike_dict)
    
    data = pd.DataFrame(datadicts)

    for i, row in data.iterrows():
        if CallPut == "C":
            if Expiry in str(row['contract']):
                x_rounded_up = round_up_to_base(int(x))
                if int(row['strike']) == x_rounded_up:
                    optionsymbol = row['contract']
                    break
                else:
                    continue
            else:
                    continue
        elif CallPut == "P":
            if Expiry in str(row['contract']):
                x_rounded_down = round_down_to_base(int(x))
                if int(row['strike']) == x_rounded_down:
                    optionsymbol = row['contract']
                    break
                else:
                    continue
            else:
                continue
        else:
            print("Failed Here")

    return optionsymbol, CallPut

def polygon_optiondata(x, from_date, to_date):
    #This is for option data
    multiplier = "15"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    optionsTicker = x
    print(optionsTicker)
    from_stamp = int(from_date.timestamp() * 1000)
    to_stamp = int(to_date.timestamp() * 1000)
    timespan = "minute"
    # from_stamp = standardize_dates(from_stamp)
    # to_stamp = standardize_dates(to_stamp)
    url = f"https://api.polygon.io/v2/aggs/ticker/{optionsTicker}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.status_code)
    response_data = json.loads(response.text)
    res_option_df = pd.DataFrame(response_data['results'])
    res_option_df['t'] = res_option_df['t'].apply(lambda x: int(x/1000))
    res_option_df['date'] = res_option_df['t'].apply(lambda x: datetime.fromtimestamp(x))
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
    print(response.status_code)
    response_data = json.loads(response.text)
    stock_df = pd.DataFrame(response_data['results'])
    stock_df['t'] = stock_df['t'].apply(lambda x: int(x/1000))
    stock_df['date'] = stock_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    underlying_price = []
    for i, row in df_optiondata.iterrows():
        matched_row = stock_df.loc[stock_df['date'] == row['date']]
        open_price = matched_row.o.values
        underlying_price.append(str(round(open_price[0],2)))
    df_optiondata['underlyingPrice'] = underlying_price
    new_date = from_date.strftime("D:" + "%Y%m%d" + "T:" + "%H-%M-%S")
    df_optiondata.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_{x}_{new_date}.csv')
    return df_optiondata

def data_pull(symbol, start_date, end_date, mktprice, strategy, contracts):
    option_symbol, direction = create_option(symbol, start_date, mktprice, strategy, contracts)
    df_tickerdata = polygon_optiondata(option_symbol, start_date, end_date)
    polygon_df = polygon_stockdata(symbol,start_date, end_date, df_tickerdata)
    return option_symbol, direction, polygon_df

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
    # results.dropna(inplace = True)
    # results = results.reset_index()   
    # index = pd.Index(days)
    # results = results.set_index(index)
    return days, datetimeindex, results

def build_dict(seq, key):
    return dict((d[key], dict(d, index=index)) for (index, d) in enumerate(seq))
