from __future__ import absolute_import, division, print_function, unicode_literals
from io import StringIO
from datetime import timedelta, date, datetime
import backtrader as bt
import backtrader.feeds as btfeeds
import itertools
import pandas as pd
import datetime as dt
import helpers.backtraderhelpers as backtest
import requests
import json
import boto3

cerebro = bt.Cerebro()
def set_cash():
    cerebro.broker.setcash(200000.0)
    print(cerebro.broker.getvalue())

def pull_data():
    rawdata = backtest.s3_data()
    start_date = datetime.strptime(rawdata['date'].values[1], '%Y-%m-%d %H:%M:%S')
    end_date = backtest.end_date(start_date, 3)
    symbol = rawdata['symbol'].values[1]
    start_price = rawdata['regularMarketPrice']
    polygon_df = backtest.polygon_data(symbol,start_date, end_date)
    return start_date, end_date, symbol, start_price, polygon_df

start_date, end_date, symbol, start_price, polygon_df = pull_data()

data = bt.feeds.GenericCSVData(
    dataname= f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/APE-Backtester-Results/Testing_Research_Data_CSV_RBLX_2023-01-02 14/00/00.csv',
    fromdate=start_date,
    todate=end_date,
    dtformat=('%Y-%m-%d %H:%M:%S'),
    datetime=9,
    open = 3,
    high = 5,
    low = 6,
    close = 4,
    volume =1, 
    openinterest=-1,
    reverse=False)

cerebro.run()

class backtraderstrat(bt.Strategy):

    def log(self, txt, dt=None):
        #Logging function for the strat
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

