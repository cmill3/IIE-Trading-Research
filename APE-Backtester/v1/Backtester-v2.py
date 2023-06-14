from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import holidays
from xone import calendar
import boto3
import helpers.backtestfunctions as backtester
import helpers.backtraderhelpers as backtrader


startdate = '05/31/2023' #MM/DD/YYYY format
enddate = '06/02/2023' #MM/DD/YYYY format
bucket_name = 'icarus-research-data'  #s3 bucket name
object_keybase = 'training_datasets/expanded_1d_datasets/' #s3 key not including date, date is added in pullcsv func

def aggregate_times(df):
    agg_function = {'Time': 'first', 'Buy': 'sum', 'Sell': 'sum', 'ActiveHoldings': 'sum', 'Cost': 'sum', 'Return': 'sum', 'TransactionCosts': 'sum', 'TransactionCostofInterval': 'sum', 'NetValueofInterval': 'sum', 'StartValue': 'sum', 'EndValue': 'sum'}
    df = df.groupby(df['Time']).aggregate(agg_function)
    df = df.drop('Time', inplace=False, axis = 1)
    return df

def pullcsvdata(startdate, enddate):
    btdays = backtrader.tradingdaterange(startdate, enddate)
    object_keydates = []

    for i in btdays:
        object_date = i + ".csv"
        object_keydates.append(object_date)
        #Need to reformat backtestfunctions to accept this info for the boto3 and polygon
    
    buysellmatchlist = []

    for i, item in enumerate(object_keydates):
        s3link = {
        'bucketname': 'icarus-research-data',
        'objectkey': f'training_datasets/expanded_1d_datasets/{item}'
        }
        startingvalue, commissioncost, rawdata, data, datetimelist, datetimeindex, res_ults, dflist, buysellpair, openlist, closelist, failed_openlist, failed_dictlist = backtester.kickoff(s3link)
        transactions, buysellmatch = backtester.btfunction(data, dflist, buysellpair, failed_openlist, failed_dictlist, datetimelist, startingvalue, commissioncost, s3link)
        if i == 0:
            res_df = transactions
            for item in buysellmatch:
                buysellmatchlist.append(item)
        elif i > 0:
            res_df = pd.concat([res_df, transactions], axis = 0)
            for item in buysellmatch:
                buysellmatchlist.append(item)
            # res_df.append(transactions)

    results = aggregate_times(res_df)
    results = results.reset_index()


    results['StartValue'][0] = int(startingvalue)
    removed_items = []
    for i, row in results.iterrows():
        results['ActiveHoldings'][i].extend(results['Buy'][i])
        if i > 0:
            results['ActiveHoldings'][i].extend(results['ActiveHoldings'][i-1])
        holdingslist = results['ActiveHoldings'][i]
        soldlist = results['Sell'][i]
        for item in soldlist:
            matchdict = next(thing for thing in buysellmatchlist if thing['CloseMarker'] == item)
            positionid = matchdict['PositionID']
            holdingslist[:] = [x for x in holdingslist if positionid not in x]
        results.at[i,'ActiveHoldings'] = holdingslist
        totaltransactioncost = sum(results['TransactionCosts'][i])
        results['TransactionCostofInterval'][i] = totaltransactioncost
        totalcost = sum(results['Cost'][i])
        totalreturn = sum(results['Return'][i])
        net = (totalreturn - totalcost) - totaltransactioncost
        results['NetValueofInterval'][i] = net
        if i > 0:
            results['StartValue'][i] = results['EndValue'][i-1]
        startval = results['StartValue'][i]
        endval = startval + net
        results['EndValue'][i] = endval
    return results

if __name__ == "__main__":
    results = pullcsvdata(startdate, enddate)
    results.to_csv('/Users/ogdiz/Projects/APE-Research/APE-Backtester/v1/BT_Results/TEST_FULL.csv')
    print(results)