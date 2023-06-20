from datetime import timedelta, datetime
import datetime as dt
import pandas as pd
import helpers.backtraderhelpers as backtest

startingvalue = backtest.startbacktrader(1000000)
commissioncost = 0.35
rawdata = backtest.s3_data()
dataset = pd.DataFrame(rawdata)
data = dataset[dataset.volume >= 20000000]
data.reset_index(inplace=True)
init_date = datetime.strptime(rawdata['date'].values[0], '%Y-%m-%d %H:%M:%S')
comp_date = backtest.end_date(init_date, 4)
datetimelist, datetimeindex, results = backtest.build_table(init_date, comp_date)

def pull_data(index):
    rawdata = backtest.s3_data()
    # dataset = pd.DataFrame(rawdata)
    # data = dataset[dataset.volume >= 20000000]
    start_date = datetime.strptime(rawdata['date'].values[index], '%Y-%m-%d %H:%M:%S')
    end_date = backtest.end_date(start_date, 3)
    symbol = rawdata['symbol'].values[index]
    mkt_price = rawdata['regularMarketPrice'].values[index]
    contracts = rawdata['contracts'].values[index]
    strategy = rawdata['title'].values[index]
    option_symbol, stratdirection, polygon_df = backtest.data_pull(symbol, start_date, end_date, mkt_price, strategy, contracts)
    open_prices = polygon_df['o'].values
    return start_date, end_date, symbol, mkt_price, strategy, option_symbol, stratdirection, polygon_df, open_prices

def BuyIterateSell(symbol, mkt_price, optionsymbol, stratdirection, open_prices, strategy):

    #option contract values
    openPrice = open_prices[0]
    open_dt = polygon_df['date'][0]
    open_datetime = open_dt.to_pydatetime()
    open_dt_hrmin = open_datetime.strftime("%Y-%m-%d %H:%M")
    contract_number = 1
    contract_size = 100
    transaction_cost = commissioncost * contract_number
    total_transaction_cost = transaction_cost * 2
    contract_cost = round((openPrice * contract_size * contract_number), 2) + transaction_cost
    inverse_date_time = []
    target_date_time = []
    openorderstr = "B"
    closeorderstr = "S"

    #Assigns target and inverse price to each strat direction
    for i, row in polygon_df.iterrows():
        if stratdirection == "C":
            targetprice = (mkt_price * 0.05) + mkt_price
            inverseprice = mkt_price - (mkt_price * 0.05)
            if float(row['underlyingPrice']) <= inverseprice:
                inverse_date_time.append(row['date'])
            elif float(row['underlyingPrice']) >= targetprice:
                target_date_time.append(row['date'])

    #Creates dictionary results if price targets have been hit'
    if len(inverse_date_time) != 0 and len(target_date_time) != 0:
        if inverse_date_time[0] <= target_date_time[0]:
            matched_row = polygon_df.loc[polygon_df['date'] == inverse_date_time[0]]
            orderSource = "max_loss"
            close_datetime = matched_row['date'].values
            closePrice = matched_open[0]
            orderType = "Sell"
            matched_dt = matched_row['date'].values
            close_dt = matched_dt[0]
            close_datetime = backtest.convertepoch(close_dt)

        elif inverse_date_time[0] >= target_date_time[0]:
            matched_row = polygon_df.loc[polygon_df['date'] == target_date_time[0]]
            orderSource = "profit_target"
            matched_open = matched_row['o'].values
            closePrice = matched_open[0]
            orderType = "Sell"
            matched_dt = matched_row['date'].values
            close_dt = matched_dt[0]
            close_datetime = backtest.convertepoch(close_dt)

        else:
            matched_row = polygon_df.iloc[-1]
            orderSource = "max_time"
            matched_open = matched_row['o'].values
            closePrice = matched_open[0]
            orderType = "Sell"
            matched_dt = matched_row['date'].values
            close_dt = matched_dt[0]
            close_datetime = backtest.convertepoch(close_dt)

    elif len(inverse_date_time) == 0 and len(target_date_time) != 0:
        matched_row = polygon_df.loc[polygon_df['date'] == target_date_time[0]]
        orderSource = "profit_target"
        matched_open = matched_row['o'].values
        closePrice = matched_open[0]
        orderType = "Sell"
        matched_dt = matched_row['date'].values
        close_dt = matched_dt[0]
        close_datetime = backtest.convertepoch(close_dt)

    elif len(inverse_date_time) != 0 and len(target_date_time) == 0:
        matched_row = polygon_df.loc[polygon_df['date'] == inverse_date_time[0]]
        orderSource = "max_loss"
        matched_open = matched_row['o'].values
        closePrice = matched_open[0]
        orderType = "Sell"
        matched_dt = matched_row['date'].values
        close_dt = matched_dt[0]
        close_datetime = backtest.convertepoch(close_dt)

    elif len(inverse_date_time) == 0 and len(target_date_time) == 0:
        #Because this is the last value in the sheet, and indexed differently, I need to use different methods to pull information
        #including not using .values and not requiring the convertepoch helper function used above
        matched_row = polygon_df.iloc[-1]
        orderSource = "max_time"
        closePrice = matched_row['o']
        orderType = "Sell"
        close_dt = matched_row['date']
        close_datetime = close_dt.to_pydatetime()

    #Defining other variables to be included in the dictionary "OrderMarker"
    orderTicker = optionsymbol
    contract_return = round((closePrice * contract_size),2) + transaction_cost
    net_value = contract_return - contract_cost
    roi = net_value / contract_cost

    PositionID = "[Symbol: " + symbol + ", Strategy: " + strategy + ", opendatetime: " + open_dt_hrmin + "]"
    UniqueOpenStr = "[Buy/Sell: " + openorderstr + ", Ticker: " + orderTicker + ", OpenPrice: " + str(openPrice) + ", PositionID: " + str(PositionID) + "]"
    UniqueCloseStr = "[Buy/Sell: " +  closeorderstr + ", Ticker: " + orderTicker + ", OpenPrice: " + str(closePrice) + ", PositionID: " + str(PositionID) + "]"

    
    OrderMarker = {
        "type": "BUY/SELL",
        "strategy": strategy,
        "source": orderSource,
        "ticker": orderTicker,
        "openPrice": openPrice,
        "closePrice": closePrice,
        "contractCost": contract_cost,
        "commissionRate": transaction_cost,
        "transactionCost": total_transaction_cost,
        "contractReturn": contract_return,
        "netValue": net_value,
        "ROI": roi,
        "openDate": open_datetime,
        "closeDate": close_datetime,
        "uniqueopenstr":UniqueOpenStr,
        "uniqueclosestr": UniqueCloseStr
        }
    
    OpenMarker = {
        "uniqueopenstr":UniqueOpenStr,
        "type": "BUY",
        "source": orderSource,
        "ticker": orderTicker,
        "openPrice": openPrice,
        "contractCost": contract_cost,
        "transactionCost": total_transaction_cost,
        "openDate": open_datetime
        }
    
    CloseMarker = {
        "uniqueclosestr": UniqueCloseStr,
        "type": "SELL",
        "ticker": orderTicker,
        "openPrice": openPrice,
        "closePrice": closePrice,
        "contractReturn": contract_return,
        "netValue": net_value,
        "ROI": roi,
        "closeDate": close_datetime
        }
    
    BuySellPair = {
        "OpenMarker": UniqueOpenStr,
        "CloseMarker": UniqueCloseStr,
        "PositionID": PositionID
        }

    # UniqueOpenMarker = {
    #     "datetime": open_datetime,
    #     "openstr": UniqueOpenStr
    # }

    # UniqueCloseMarker = {
    #     "datetime": close_datetime,
    #     "closestr": UniqueCloseStr
    # }

    return OrderMarker, OpenMarker, CloseMarker, open_datetime, close_datetime, BuySellPair

dflist = []
BuySellMatch = []
openlist = []
closelist = []
failed_openlist = []
failed_dictlist = []

for i, rows in data.iterrows():
    try:
        start_date, end_date, symbol, mkt_price, strategy, optionsymbol, stratdirection, polygon_df, open_prices = pull_data(i)
        OrderMarker, OpenMarker, CloseMarker, open_datetime, close_datetime, BuySellPair = BuyIterateSell(symbol, mkt_price, optionsymbol, stratdirection, open_prices, strategy)
        dflist.append(OrderMarker)
        BuySellMatch.append(BuySellPair)
        # openlist.append(UniqueOpenMarker)
        # closelist.append(UniqueCloseMarker)
        # if i >= 100:
        #     break
        print(str(i) + "/" + str(len(data.index)))
    except:
        failed_openlist.append(optionsymbol)
        print("Failed to pull option data for " + optionsymbol)
        print(str(i) + "/" + str(len(data.index)))
        continue

df_trades = pd.DataFrame(dflist)
# df_open = pd.DataFrame(openlist)
# df_close = pd.DataFrame(closelist)

key_list = ["Time", "Buy", "Sell", "ActiveHoldings", "Cost", "Return", "TransactionCosts", "TransactionCostofInterval", "NetValueofInterval", "StartValue", "EndValue"]
n = len(datetimelist)
transactiondict = []
for idx in range(0, n, 1):
    transactiondict.append({
        key_list[0]: datetimelist[idx],
        key_list[1]: [],
        key_list[2]: [],
        key_list[3]: [],
        key_list[4]: [],
        key_list[5]: [],
        key_list[6]: [],
        key_list[7]: [],
        key_list[8]: 0,
        key_list[9]: 0,
        key_list[10]: 0
        })
    
reference = backtest.build_dict(transactiondict, key="Time")

for i, row in df_trades.iterrows():
    try:
        open_info = reference.get(row['openDate'])
        close_info = reference.get(row['closeDate'])
        open_index = open_info['index']
        close_index = close_info['index']
        openreferencedate = open_info['Time']
        closereferencedate = close_info['Time']
        transactiondict[open_index]['Buy'].append(row['uniqueopenstr'])
        transactiondict[close_index]['Sell'].append(row['uniqueclosestr'])
        transactiondict[open_index]['Cost'].append(row['contractCost'])
        transactiondict[close_index]['Return'].append(row['contractReturn'])
        transactiondict[open_index]['TransactionCosts'].append(row['commissionRate'])
        transactiondict[close_index]['TransactionCosts'].append(row['commissionRate'])
    except Exception as e:
        print(e)
        print("Failed to add a trade to dict for index" + str(i))
        failed_dictlist.append({'OpenStr': row['uniqueopenstr'], 'CloseStr': row['uniqueclosestr']})
        continue


transactions = pd.DataFrame(transactiondict)

transactions['StartValue'][0] = int(startingvalue)

for i, row in transactions.iterrows():
    transactions['ActiveHoldings'][i].extend(transactions['Buy'][i])
    if i > 0:
        transactions['ActiveHoldings'][i].extend(transactions['ActiveHoldings'][i-1])
    holdingslist = transactions['ActiveHoldings'][i]
    soldlist = transactions['Sell'][i]
    for item in soldlist:
        matchdict = next(thing for thing in BuySellMatch if thing['CloseMarker'] == item)
        positionid = matchdict['PositionID']
        holdingslist[:] = [x for x in holdingslist if positionid not in x]
    transactions.at[i,'ActiveHoldings'] = holdingslist
    totaltransactioncost = sum(transactions['TransactionCosts'][i])
    transactions['TransactionCostofInterval'][i] = totaltransactioncost
    totalcost = sum(transactions['Cost'][i])
    totalreturn = sum(transactions['Return'][i])
    net = (totalreturn - totalcost) - totaltransactioncost
    transactions['NetValueofInterval'][i] = net
    if i > 0:
        transactions['StartValue'][i] = transactions['EndValue'][i-1]
    startval = transactions['StartValue'][i]
    endval = startval + net
    transactions['EndValue'][i] = endval

print(failed_dictlist)
print(len(failed_dictlist))
print(len(dflist))
print(failed_openlist)

transactions.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/v1/BT_Results/TEST.csv')