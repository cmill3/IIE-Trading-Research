from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import holidays
from xone import calendar
import boto3
import helpers.backtestfunctions as backtester
import helpers.backtraderhelpers as backtrader

startdate = '04/03/2023' #MM/DD/YYYY format
enddate = '04/06/2023' #MM/DD/YYYY format
bucket_name = 'icarus-research-data'
object_keybase = 'training_datasets/expanded_1d_datasets/'

def pullcsvdata(startdate, enddate):
    btdays = backtrader.tradingdaterange(startdate, enddate)
    object_keydates = []
    for i in btdays:
        object_date = i + ".csv"
        object_keydates.append(object_date)
        #Need to reformat backtestfunctions to accept this info for the boto3 and polygon
    for i, item in enumerate(object_keydates):
        s3link = {
        'bucketname': 'icarus-research-data',
        'objectkey': f'training_datasets/expanded_1d_datasets/{item}'
        }
        startingvalue, commissioncost, rawdata, data, datetimelist, datetimeindex, results, dflist, buysellmatch, openlist, closelist, failed_openlist, failed_dictlist = backtester.kickoff(s3link)
        transactions = backtester.btfunction(data, dflist, buysellmatch, failed_openlist, failed_dictlist, datetimelist, startingvalue, commissioncost, s3link)
        print(transactions)
        if i == 0:
            results = transactions
        if i > 0:
            results = pd.concat([results, transactions], axis = 1)
            # results.append(transactions)

    results.to_csv('/Users/ogdiz/Projects/APE-Research/APE-Backtester/v1/BT_Results/TEST_FULL.csv')
    return results

results = pullcsvdata(startdate, enddate)

