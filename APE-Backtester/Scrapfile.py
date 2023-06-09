from datetime import datetime, timedelta
from io import StringIO
import os
import boto3
import pandas as pd
import requests
import json
import ast
import holidays

results = pd.read_csv('/Users/ogdiz/Projects/APE-Research/APE-Backtester/v1/BT_Results/IGNORE.csv')

results['StartValue'][0] = 1000000

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

print(results)