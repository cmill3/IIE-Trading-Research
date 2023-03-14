import backtrader as bt
import backtrader.feeds as btfeeds
import pandas as pd
from datetime import datetime, timedelta, date
from matplotlib import warnings
import os.path
import sys
import yfinance as yf
import numpy as np
import datetime as dt
from BTFunctions import end_date_trading_days_only
import requests
import json
from yahooquery import Ticker
from pandas.io.json import json_normalize


##################
##################
##################
#OPTION CONTRACT BUILDER AND RESEARCH DATA BUILDER


#Pulling sample data from S3 in and creating a data frame with the classifier -- Pulling in Data from Gainer/Loser Models
df = pd.read_csv('/Users/ogdiz/Projects/APE-Research/APE-Backtester/2023_13_00_gainers_Sample.csv', usecols=['symbol','classifier_prediction','current_date'])
df.dropna(inplace = True)
df = df.reset_index()
print(df)

#Builidng the ticker & start time/end time (Date/Time format will be YYYY-MM-DD, HH:MM:SS)
data_start_date = df['current_date'][0] #This just pulls the first row's date since all of the dates will be the same in a given CSV
full_name = []
start_time_from_csv = []
end_time_post_csv = []
for filename in os.listdir('/Users/ogdiz/Projects/APE-Research/APE-Backtester'):
    if filename.endswith('.csv'):
        # full_name.append(pd.read_csv(filename,sep="|"))
        start_time_from_csv.append(filename[5:7]+":"+filename[8:10])
        full_name.append(filename)
start_time = start_time_from_csv

data_start_date_and_time = data_start_date + " " + start_time[0] + ":00"

backtester_start_datetime = datetime.strptime(data_start_date_and_time, '%Y-%m-%d %H:%M:%S')

#This is all of the data to create the end data of each time period, not needed to build the option data
#Add holidays to this so we dont get end dates that are on non-trading days, can create a df of all trading days 

def end_date_trading_days_only(from_date, add_days): #End date, n days later for the data set built to include just trading days, but doesnt filter holidays
    trading_days_to_add = add_days
    current_date = from_date
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    backtester_end_datetime.append(current_date)

#Parameters for the function above
one_day_added = 1 #For the historical hourly ticker data, used in finding the value that we will use
n_days_added = 3 #For the backtester training set
backtester_end_datetime = []

#Creating all of the relevent dates used throughout here
end_date_trading_days_only(backtester_start_datetime, n_days_added)
end_date_trading_days_only(backtester_start_datetime, one_day_added)
backtester_start_date_notime = backtester_start_datetime.strftime('%Y-%m-%d')
backtester_end_date_notime = backtester_end_datetime[0].strftime('%Y-%m-%d')
backtester_end_date_notime_one_day_later = backtester_end_datetime[1].strftime('%Y-%m-%d')
d = datetime.now() # Monday
t_2wk = timedelta((11 - d.weekday()) % 14)
expiry_date_no_time = datetime.strptime(backtester_start_date_notime, '%Y-%m-%d')
t_2wk_option = timedelta((11 - expiry_date_no_time.weekday()) % 14)
Expiry_Date_for_data = ((expiry_date_no_time + t_2wk_option).strftime('%Y-%m-%d'))
Expiry_Date_for_data_str = datetime.strptime(Expiry_Date_for_data, '%Y-%m-%d')
backtester_data_start_date = Expiry_Date_for_data_str.strftime('%y%m%d')
millisec_start_time = int(backtester_start_datetime.timestamp() * 1000)
millisec_end_time = int(backtester_end_datetime[0].timestamp() * 1000)

#Variables needed to pull in the data to be used as the trading data
Symbol_Value = df['symbol']
Trend_Value = []
Expiry_Date = []

#Variables/Lists that the functions will place values into to be used for other functions or to be added to a df or CSV
sym_list = []
cp_list = []
date_list_2wk = []
strike_price_list = []
option_ticker_list = []
successful_ticker_list = []
option_chain_processing_status = []
historical_ticker_value = []
historical_OOTM_strike_price_call = []
historical_OOTM_strike_price_put = []
time_frame_values = []

#Function to round values to the nearest 5 value for clean option ticker selections, round up for Calls and round down for Puts
def round_up_to_base(x, base=5):
    return x + (base - x) % base
def round_down_to_base(x, base=5):
    return x - (x % base)

#Just adding the symbols from df(from CSV) into a list
def Symbol(x):
    sym_list.append(Symbol_Value[i])

#This is just pulling historical stock price data from the time/date that the signal came through for each ticker/row, so we know what the PIT(point in time) value was for a stock - this will be used as the basis to pick the option that is OOTM, generates the OOTM stock by multiplying by 0.025 now but this will change
def polygon_ticker_call(x):
    multiplier = "1"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    symbol = x
    from_stamp = backtester_start_date_notime
    to_stamp = backtester_end_date_notime_one_day_later
    timespan = "hour"
    # from_stamp = standardize_dates(from_stamp)
    # to_stamp = standardize_dates(to_stamp)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_stamp}/{to_stamp}?adjusted=true&sort=asc&limit={limit}&apiKey={key}"
    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.status_code)
    # Storing the ticker data from that date and time
    # Manipulating the ticker data to give us a historical strike that is OOTM for a call and a put, respectively
    response_data = json.loads(response.text)
    res_df = pd.DataFrame(response_data['results'])
    res_df['t'] = res_df['t'].apply(lambda x: int(x/1000))
    res_df['date'] = res_df['t'].apply(lambda x: datetime.fromtimestamp(x))
    his_tic_val = res_df.loc[(res_df['date'] == backtester_start_datetime)].o
    print(round(his_tic_val), "LOOK HERE")
    historical_ticker_value.append(round(his_tic_val))

#Will turn GAINERS into Calls and LOSERS into PUTS
def Trend(full_name):
    if 'losers' in full_name:
        Trend_Value.append("-1")
        cp_list.append("PUT")
        print("LOSER")
    elif 'gainers' in full_name:
        Trend_Value.append("1")
        cp_list.append("CALL")
        print("GAINER")
    else:
        cp_list.append("Trend Processing Error: Review Pathname Data and Code")
        print("Trend Processing Error: Review TB Data and Code")

#Necessary to call this trend function once to assign a Gainers/Losers profile to the options

Trend(full_name[-1])

#Generates the Current Option chain data, pulls the value that correlates to a the previously pulled stock price, and the strike price that is OOTM, and modifies the ticker to insert the historical date and generate our option ticker
def options_chain_data_processing(x, i):
    Tick = Ticker(x) #pd series.value
    df_ocraw = Tick.option_chain #pulling the data into a data frame (optionchainraw = ocraw)
    Expiry_Date_OC = ((d + t_2wk).strftime('%Y-%m-%d'))
    if cp_list[0] == "PUT":
        contract_type = "puts"
    elif cp_list[0] == "CALL":
        contract_type = "calls"
    else:
        print("OOTM STRIKE PRICE PROCESSING ERROR")
    df_optionchain_2wk = df_ocraw.loc[x, Expiry_Date_OC, contract_type]
    num = len(df_optionchain_2wk) #This (and the next 2 lines of code) is used to create an index that will be used to find one position OOTM, only necessary because yahooquery data is datetime indexed
    num_index = [*range(0, num, 1)]
    df_optionchain_2wk['num_index'] = num_index
    option_chain_processing_status.append("Success")
    print(option_chain_processing_status)
    if contract_type == "calls":
        strike_to_match = int(historical_ticker_value[i].values)
        index_pull = df_optionchain_2wk[(df_optionchain_2wk['strike'] == strike_to_match)].num_index
        new_index_value = int(index_pull[0] + 1)
        strike_price = df_optionchain_2wk[(df_optionchain_2wk['num_index'] == new_index_value)].strike
        print(strike_price)
        # strike_price = df_optionchain_2wk[(df_optionchain_2wk['strike'] == strike_to_match)].strike
        # print(strike_price)
        option_tick = df_optionchain_2wk[(df_optionchain_2wk['num_index'] == new_index_value)].contractSymbol
        option_tick_value = str(option_tick[0])
        if len(x) == 1:
            new_option_ticker = option_tick_value[:1] + backtester_data_start_date + option_tick_value[7:]
        elif len(x) == 2:
            new_option_ticker = option_tick_value[:2] + backtester_data_start_date + option_tick_value[8:]
        elif len(x) == 3:
            new_option_ticker = option_tick_value[:3] + backtester_data_start_date + option_tick_value[9:]
        elif len(x) == 4:
            new_option_ticker = option_tick_value[:4] + backtester_data_start_date + option_tick_value[10:]
        elif len(x) == 5:
            new_option_ticker = option_tick_value[:5] + backtester_data_start_date + option_tick_value[11:]
        else:
            print("Ticker string splicing for historical date failed")    
        option_ticker = "O:" + str(new_option_ticker)
        print(option_ticker)
        strike_price_list.append(strike_price[0])
        option_ticker_list.append(option_ticker)
        successful_ticker_list.append(x)
    elif contract_type == "puts":
        strike_to_match = int(historical_ticker_value[i].values)
        index_pull = df_optionchain_2wk[(df_optionchain_2wk['strike'] == strike_to_match)].num_index
        new_index_value = int(index_pull[0] - 1)
        strike_price = df_optionchain_2wk[(df_optionchain_2wk['num_index'] == new_index_value)].strike
        print(strike_price)
        # strike_price = df_optionchain_2wk[(df_optionchain_2wk['strike'] == strike_to_match)].strike
        # print(strike_price)
        option_tick = df_optionchain_2wk[(df_optionchain_2wk['num_index'] == new_index_value)].contractSymbol
        option_tick_value = str(option_tick[0])
        if len(x) == 1:
            new_option_ticker = option_tick_value[:1] + backtester_data_start_date + option_tick_value[7:]
        elif len(x) == 2:
            new_option_ticker = option_tick_value[:2] + backtester_data_start_date + option_tick_value[8:]
        elif len(x) == 3:
            new_option_ticker = option_tick_value[:3] + backtester_data_start_date + option_tick_value[9:]
        elif len(x) == 4:
            new_option_ticker = option_tick_value[:4] + backtester_data_start_date + option_tick_value[10:]
        elif len(x) == 5:
            new_option_ticker = option_tick_value[:5] + backtester_data_start_date + option_tick_value[11:]
        else:
            print("Ticker string splicing for historical date failed")    
        option_ticker = "O:" + str(new_option_ticker)
        print(option_ticker)
        strike_price_list.append(strike_price[0])
        option_ticker_list.append(option_ticker)
        successful_ticker_list.append(x)
    else:
        print("OOTM OPTION NAME & STRIKE PRICE PROCESSING ERROR")

#Utilizes the option ticker created from the option chain to pull the data, filters the results, changes millisecond time to UTC, and appends the data to a df & CSV that is uniquely named to correlate to the option ticker and start date/time - currently saved locally but when implemented in AWS, will save to S3 for a generalized log of the research data that we are using -- this will be used int he backtester as timeframe data
def polygon_option_call(x):
    multiplier = "30"
    limit = 50000
    key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"
    payload={}
    headers = {}
    optionsTicker = x
    from_stamp = millisec_start_time
    to_stamp = millisec_end_time
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
    print(res_option_df)
    res_option_df.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-BAcktester/APE-Backtester-Results/Testing_Research_Data_CSV_{x}_{backtester_start_datetime}.csv')

#This loop iterates through the tickers pulled in from the Gainers/Loisers CSV on AWS S3 and generates all of the data needed by the Polygon option API Call to get the needed testing data
for i, row in df.iterrows():
    Symbol(Symbol_Value.iloc[i])
    polygon_ticker_call(Symbol_Value.iloc[i])
    #options_chain_data_processing(Symbol_Value.iloc[i], i)
    try:
        options_chain_data_processing(Symbol_Value.iloc[i], i)
    except:
        print("Option Chain Processing failed for " + str(Symbol_Value.iloc[i]) + ". Check option expiry date spacing for more context (i.e. weekly, biweekly, etc) or manually review option in question.")
        continue

#Creating new dataframe that will take the values from the polygon historical ticker call and the yahooquery ticker symbol to create new list that will be iterated through to pull Polygon historical option data
df_successful_ticker_list = pd.DataFrame(successful_ticker_list, columns = ['Ticker'])
df_strike_price_list = pd.DataFrame(strike_price_list, columns = ['Strike_Price'])
df_option_ticker_list = pd.DataFrame(option_ticker_list, columns = ['Contract_Name'])
df_option_data = pd.concat([df_successful_ticker_list, df_strike_price_list, df_option_ticker_list], axis=1)
print(df_option_data)


#This utilizes the successful df_options_data values that get pulled in and iterates through to create CSVs with the relevent data for the backtester
for i, row in df_option_data.iterrows():
   polygon_option_call(option_ticker_list[i])

##################
##################
##################
# BACKTRADER BEGINS HERE

#Cornerstone of backtrader because it gathers data/strates, executes backtesting, returns results, and gives access to plotting facilities



#Pulling the information from the relevent BT data, will use the DF created by the first to intterae through thiis
# In this function x = option ticker, and y = backtester_start_datetime
        
# cerebro = bt.Cerebro()

# data = bt.feeds.GenericCSVData(
#     dataname= f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_O:BIDU230113C00140000_2023-01-05 13:00:00.csv',
#     fromdate=backtester_start_datetime,
#     todate=backtester_end_datetime[0],
#     timeframe=-1,
#     nullvalue=0.0,
#     dtformat=('%Y-%m-%d %H:%M:%S'),
#     datetime=9,
#     open = 3,
#     high = 5,
#     low = 6,
#     close = 4,
#     volume =1, 
#     openinterest=-1,
#     reverse=False)
    

# cerebro.adddata(data)


# cerebro.broker.setcash(100000)
# cerebro.broker.setcommission(commission=0.65)
# print("Cash & Commission has been set")

# cerebro.run()
# cerebro.plot()

# def Backtester_Function(x,y):
#     cerebro = bt.Cerebro()

#     data = bt.feeds.GenericCSVData(
#        dataname= f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_{x}_{y}.csv',
#        fromdate=backtester_start_datetime,
#        todate=backtester_end_datetime,
#        timeframe=-1,
#        nullvalue=0.0,
#        dtformat=('%Y-%m-%d %H:%M:%S'),
#        datetime=9,
#        open = 3,
#        high = 5,
#        low = 6,
#        close = 4,
#        volume =1, 
#        openinterest=-1,
#        reverse=False)
       
    
#     cerebro.adddata(data)

    
#     cerebro.broker.setcash(100000)
#     cerebro.broker.setcommission(commission=0.65)
#     print("Cash & Commission has been set")

#     cerebro.addstrategy(SimpleMA)
#     cerebro.run()
#     cerebro.plot()


# #This utilizes the successful df_options_data values that get pulled in and iterates through to create CSVs with the relevent data for the backtester
# for i, row in df_option_data.iterrows():
#     polygon_option_call(option_ticker_list[i])
#     Backtester_Function(option_ticker_list[i], backtester_start_datetime)

