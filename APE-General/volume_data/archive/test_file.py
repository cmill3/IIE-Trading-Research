import pandas as pd
import numpy as np
import boto3
import requests
import ast
import os
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import pytz
import concurrent.futures
import pandas_market_calendars as mcal
from volume_helper import *

# def pull_monthly_sp500_constituents_s3(path, bucket):
#     date = datetime.today().strftime('%Y/%m')
#     new_path = path + '/' + date + '/'
#     keys = s3.list_objects(Bucket=bucket,Prefix=f"{new_path}")["Contents"]
#     sp_object = keys[-1]
#     key = sp_object['Key']
#     print(key)
#     dataset = s3.get_object(Bucket=bucket,Key=f"{key}")
#     df = pd.read_csv(dataset.get("Body"))
#     df.drop(columns=['Unnamed: 0'],inplace=True)
#     df.reset_index(drop=True)
#     return df


# bucket = 'icarus-research-data'
# key = 'sp500-constituents'

# df = pull_monthly_sp500_constituents_s3(key, bucket)
# print(df)


df = pd.read_csv('/Users/diz/Downloads/sp500-list.csv')
new_df = df.pivot(columns='symbols')
print(new_df)