from inv_backtesters import backtester_v2 as backtester
import pandas as pd
import boto3
from datetime import datetime, timedelta


def build_experiment(start_date, end_date):
    date_list = build_date_list(start_date, end_date)
    results = backtester.run_backtest(start_date.strftime("%Y/%m/%d"), end_date.strftime("%Y/%m/%d""))
    for date_str in date_list:
        print(date_str)
        run(date_str)



def build_date_list(start_date, end_date):
    date_diff = end_date - start_date
    numdays = date_diff.days 
    date_list = []
    print(numdays)
    for x in range (0, numdays):
        temp_date = start_date + timedelta(days = x)
        if temp_date.weekday() < 5:
            date_str = temp_date.strftime("%Y/%m/%d")
            date_list.append(date_str)

    return date_list

def build_dataset(date_list):

if __name__ == "__main__":
    start_date = datetime(2021,7,1)
    end_date = datetime(2022,1,11)