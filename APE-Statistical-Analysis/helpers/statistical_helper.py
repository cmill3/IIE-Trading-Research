import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import pytz
import json
import helpers.ta_formulas as ta
from helpers.helper import *
from helpers.statistical_helper import *
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import boto3
import ast

key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"

def statistical_dates_prep(row):
    start_date_str = (row['order_transaction_date']).split('T')[0]
    open_date = (row['order_transaction_date'].split('T')[0]) + " " + ((row['order_transaction_date'].split('T')[1]).split('.')[0])[:-3]

    multiplier = 30
    timespan = 'minute'

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    weekday = start_date.weekday()
    days_till_friday = (4 - weekday) % 7
    end_date = start_date + timedelta(days = days_till_friday)
    end_date_str = end_date.strftime('%Y-%m-%d')

    return start_date_str, open_date, start_date, weekday, days_till_friday, end_date, end_date_str


def stock_and_options_agg(ticker, options_contract, start_date_str, end_date_str, multiplier, timespan, open_date, qty):
    df = polygon_call_stocks(ticker, start_date_str, end_date_str, multiplier, timespan, open_date)
    df2 = polygon_call_options(options_contract, start_date_str, end_date_str, multiplier, timespan, open_date)
    base_df = pd.merge(df, df2, on='dt')
    base_df = base_df[['symbol', 'option_contract', 'dt', 'underlying_price', 'underlying_volume', 'option_price', 'option_volume']]
    base_df['contracts'] = qty
    base_df['contract_value'] = base_df['option_price'].apply(lambda x: round(float(x) * float(qty)))
    base_df['underlying_value'] = base_df['underlying_price'].apply(lambda x: round(float(x) * float(qty)))
    return base_df

def window_prep(ticker, start_date_str, end_date_str, timespan, multiplier):
    window_df = stat_window_creator(ticker, start_date_str, end_date_str, timespan, multiplier)
    window_df['dt'] = window_df['date'].apply(lambda x: x.tz_localize(None))
    window_df.reset_index(drop = True, inplace=True)
    window_df.set_index('dt',inplace=True)
    return window_df

def rsi_roc(base_df, window_df):
    rsi = ta.rsi(window_df['c'], window=15)
    roc = ta.roc(window_df['c'], window=15)
    dataroc_df = pd.merge(base_df, roc, on = 'dt')
    dataroc_df.rename(columns = {'c':'roc'}, inplace=True)
    final_df = pd.merge(dataroc_df, rsi, on = 'dt')
    final_df.rename(columns = {'c':'rsi'}, inplace=True)
    return final_df

def stat_correlation(final_df):
    rsi_contract_cost_corr = final_df['rsi'].corr(final_df['contract_value'])
    roc_contract_cost_corr = final_df['roc'].corr(final_df['contract_value'])
    rsi_underlying_value_corr = final_df['rsi'].corr(final_df['underlying_price'])
    roc_underlying_value_corr = final_df['roc'].corr(final_df['underlying_price'])  
    roc_rsi_dict ={
        'rsi_contract_cost_corr': rsi_contract_cost_corr,
        'roc_contract_cost_corr': roc_contract_cost_corr,
        'rsi_underlying_value_corr': rsi_underlying_value_corr,
        'roc_underlying_value_corr': roc_underlying_value_corr
    }
    return roc_rsi_dict