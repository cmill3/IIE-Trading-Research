import backtrader as bt
import backtrader.feeds as btfeeds
import pandas as pd
from datetime import datetime, timedelta, date
from matplotlib import warnings
import sys
import yfinance as yf
import numpy as np
import datetime as dt
from BTFunctions import end_date_trading_days_only
import requests
import json
from yahooquery import Ticker
import boto3
from smart_open import smart_open
import os

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

bucket_name = 'icarus-research-data'
object_key = 'training_datasets/raw_1d_datasets/2023/01/02.csv'
obj = s3.get_object(Bucket = bucket_name, Key = object_key)
data = obj['Body'].read().decode('utf-8')
print(data)
df = pd.read_csv(data, sep=',')
print(df)