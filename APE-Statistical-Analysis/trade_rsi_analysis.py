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

s3 = boto3.client('s3')
key = "A_vXSwpuQ4hyNRj_8Rlw1WwVDWGgHbjp"

def statistical_analysis_coordinator(row):
    multiplier = 30
    timespan = 'minute'
    ticker = row['underlying_symbol']
    option_contract = 'O:' + row['option_symbol']
    open_volume = row['qty_executed_open']
    start_date_str, open_date, start_date, weekday, days_till_friday, end_date, end_date_str = statistical_dates_prep(row)
    base_df = stock_and_options_agg(ticker, option_contract, start_date_str, end_date_str, multiplier, timespan, open_date, open_volume)
    window_df = window_prep(ticker, start_date_str, end_date_str, timespan, multiplier)
    final_df = rsi_roc(base_df,window_df)
    roc_rsi_dict = stat_correlation(final_df)
    roc_rsi_dict['winloss'] = row['winloss']
    return roc_rsi_dict

def pull_orders_clean_df(path, bucket):
    df = pull_closed_orders_s3(path, bucket)
    orders_list = df['open_order_id'].to_list()
    orders_df, errors = get_open_trades_by_orderid(orders_list)
    orders_df['average_fill_price_close_float'] = orders_df['average_fill_price_close'].astype(float)
    orders_df['average_fill_price_open_float'] = orders_df['average_fill_price_open'].astype(float)
    orders_df['pnlcoefficient'] = orders_df['average_fill_price_close_float'] - orders_df['average_fill_price_open_float']
    orders_df['winloss'] = np.where(orders_df['pnlcoefficient'] < 0, 'L', 'W')


def pull_closed_orders(month, year):
    key = f"monthly/icarus-closed-orders-table-inv/scheduled_monthly_log_icarus-closed-orders-table-inv{year}_{month}.csv"
    orders = s3.get_object(Bucket="closed-orders-log", Key=key)
    orders = pd.read_csv(orders['Body'])
    orders_df = orders[orders['env'] == 'PROD_VAL']
    return orders_df



if __name__ == '__main__':
    path = 'closed_orders/PROD_VAL/CDBFC_1D'
    bucket = 'inv-alerts-trading-data'

    df = pull_closed_orders_s3(path, bucket)

    df = pd.read_csv('/Users/diz/Documents/Projects/APE-Research/APE-Statistical-Analysis/sample_csv/sample_roc_cdbfc1d.csv')
    stat_list = []
    for i, row in df.iterrows():
        roc_rsi_dict = statistical_analysis_coordinator(row)
        stat_list.append(roc_rsi_dict)
        print(roc_rsi_dict)

    final_df = pd.DataFrame(stat_list)
    final_df.to_csv('/Users/diz/Documents/Projects/APE-Research/APE-Statistical-Analysis/sample_csv/roc_rsi_sample.csv')







