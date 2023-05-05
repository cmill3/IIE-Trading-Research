from datetime import timedelta, datetime
import datetime as dt
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
import pandas as pd
import helpers.backtraderhelpers as backtest
import matplotlib

def StartBacktrader(Starting_Cash):
    Starting_Value = Starting_Cash
    return Starting_Value

CurrentValue = StartBacktrader(1000000)
rawdata = backtest.s3_data()
dataset = pd.DataFrame(rawdata)
data = dataset[dataset.volume >= 20000000]
results_start = datetime.strptime(rawdata['date'].values[index], '%Y-%m-%d %H:%M:%S')
results_end = backtest.end_date(results_start, 3)
results = backtest.build_table(results_start,results_end)

def pull_data(index):
    rawdata = backtest.s3_data()
    start_date = datetime.strptime(rawdata['date'].values[index], '%Y-%m-%d %H:%M:%S')
    end_date = backtest.end_date(start_date, 3)
    symbol = rawdata['symbol'].values[index]
    mkt_price = rawdata['regularMarketPrice'].values[index]
    contracts = rawdata['contracts'].values[index]
    strategy = rawdata['title'].values[index]
    option_symbol, stratdirection, polygon_df = backtest.data_pull(symbol, start_date, end_date, mkt_price, strategy, contracts)
    open_prices = polygon_df['o'].values
    return start_date, end_date, symbol, mkt_price, strategy, option_symbol, stratdirection, polygon_df, open_prices

def BuyIterateSell(current_value, mkt_price, optionsymbol, stratdirection, open_prices):

    #option contract values
    # openPrice = open_prices[0]
    # open_datetime = polygon_df['date'][0]
    # contract_size = 100
    # contract_cost = openPrice * contract_size
    # value_after_buy = current_value - contract_cost
    openPrice, open_datetime, contract_size, contract_cost, value_after_buy = open_prices[0], polygon_df['date'][0], 100, openPrice * contract_size, current_value - contract_cost
    #underlyingvalues
    inverse_date_time = []
    target_date_time = []

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
            matched_open = matched_row['o'].values
            closePrice = matched_open[0]
            orderType = "Sell"
            close_datetime = matched_row.date
        elif inverse_date_time[0] >= target_date_time[0]:
            matched_row = polygon_df.loc[polygon_df['date'] == target_date_time[0]]
            orderSource = "profit_target"
            matched_open = matched_row['o'].values
            closePrice = matched_open[0]
            orderType = "Sell"
            close_datetime = matched_row.date
        else:
            matched_row = polygon_df.iloc[-1]
            orderSource = "max_time"
            matched_open = matched_row['o'].values
            closePrice = matched_open[0]
            orderType = "Sell"
            close_datetime = matched_row.date
    elif len(inverse_date_time) == 0 and len(target_date_time) != 0:
        matched_row = polygon_df.loc[polygon_df['date'] == target_date_time[0]]
        orderSource = "profit_target"
        matched_open = matched_row['o'].values
        closePrice = matched_open[0]
        orderType = "Sell"
        close_datetime = matched_row.date
    elif len(inverse_date_time) != 0 and len(target_date_time) == 0:
        matched_row = polygon_df.loc[polygon_df['date'] == inverse_date_time[0]]
        orderSource = "max_loss"
        matched_open = matched_row['o'].values
        closePrice = matched_open[0]
        orderType = "Sell"
        close_datetime = matched_row.date
    elif len(inverse_date_time) == 0 and len(target_date_time) == 0:
        matched_row = polygon_df.iloc[-1]
        orderSource = "max_time"
        closePrice = matched_row['o']
        orderType = "Sell"
        close_datetime = matched_row.date


    #Defining other variables to be included in the dictionary "OrderMarker"
    orderTicker = optionsymbol
    contract_return = closePrice * contract_size
    net_value = contract_return - contract_cost
    value_after_sell = value_after_buy + contract_return
    roi = net_value / contract_cost
    
    OrderMarker = {
        "type": "BUY/SELL",
        "source": orderSource,
        "ticker": orderTicker,
        "openPrice": openPrice,
        "closePrice": closePrice,
        "contractCost": contract_cost,
        "contractReturn": contract_return,
        "netValue": net_value,
        "ROI": roi,
        "openDate": open_datetime,
        "closeDate": close_datetime
        }
    
    OpenMarker = {
        "type": "BUY",
        "source": orderSource,
        "ticker": orderTicker,
        "openPrice": openPrice,
        "contractCost": contract_cost,
        "openDate": open_datetime
        }
    
    CloseMarker = {
        "type": "SELL",
        "ticker": orderTicker,
        "openPrice": openPrice,
        "closePrice": closePrice,
        "contractReturn": contract_return,
        "netValue": net_value,
        "ROI": roi,
        "closeDate": close_datetime
        }
    
    for

    return OrderMarker, OpenMarker, CloseMarker

dflist = []
failed_list = []

for i, rows in data.iterrows():
    try:
        start_date, end_date, symbol, mkt_price, strategy, optionsymbol, stratdirection, polygon_df, open_prices = pull_data(i)
        OrderMarker, OpenMarker, CloseMarker = BuyIterateSell(mkt_price, optionsymbol, stratdirection, open_prices)
        dflist.append(OrderMarker)

    except:
        failed_list.append(optionsymbol)
        print("Failed to pull option data for" + optionsymbol)
        continue


df_trades = pd.DataFrame(dflist)
start_str = start_date.strftime('%m-%d-%Y--%H:%M:%S')
end_str = end_date.strftime('%m-%d-%Y--%H:%M:%S')
df_trades.to_csv(f'/Users/ogdiz/Projects/APE-Research/APE-Backtester/v1/BT_Results/{start_str}-{end_str}.csv')



for i, row in df_trades.iterrows():
    if df_trades['openDate'] == results['Time']:
        open_dict = {
            'openprice': df_trades['openDate'].openPrice
        }


#create a dataframe for every 15-30 minute interval and then have these values adding to a specific row for the open and close times
