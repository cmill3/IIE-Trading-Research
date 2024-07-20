import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import pytz
import json
import ta_formulas as ta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import boto3
import ast

s3 = boto3.client('s3')

def pull_closed_orders(month, year):
    key = f"monthly/icarus-closed-orders-table-inv/scheduled_monthly_log_icarus-closed-orders-table-inv{year}_{month}.csv"
    orders = s3.get_object(Bucket="closed-orders-log", Key=key)
    orders = pd.read_csv(orders['Body'])
    # orders_df = orders['order_id'].unique()
    orders_df = orders[orders['env'] == 'PROD_VAL']
    return orders_df

# def clean_closed_orders(orders):



if __name__ == '__main__':
    month = '4' #As of writing this code, 4-7 works
    year = '2024' #Only 2024 works
    orders = pull_closed_orders(month, year)
    print(len(orders))
    # print(orders.columns)






