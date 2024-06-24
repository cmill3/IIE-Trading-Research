from datetime import timedelta, datetime
import datetime as dt
import pandas as pd
from helpers import backtrader_helper
import warnings
import helpers.momentum_strategies as trade
import boto3
import pytz
from helpers.constants import *
import math

s3 = boto3.client('s3')


def pull_data_invalerts(bucket_name, object_key, file_name, prefixes, time_span):
    dfs = []
    for prefix in prefixes:
        try:
            print(f"{object_key}/{prefix}/{file_name}")
            obj = s3.get_object(Bucket=bucket_name, Key=f"{object_key}/{prefix}/{file_name}")
            df = pd.read_csv(obj.get("Body"))
            df['strategy'] = prefix
            dfs.append(df)
        except Exception as e:
            print(f"no file for {prefix} with {e}")
            continue
    data = pd.concat(dfs)
    data = data[data.predictions == 1]
    start_time = datetime.strptime(data['date'].values[0], '%Y-%m-%d')
    end_date = backtrader_helper.create_end_date_local_data(data['date'].values[-1], time_span)
    datetime_list, datetime_index, results = backtrader_helper.create_datetime_index(start_time, end_date)
    return data, datetime_list


def create_simulation_data_inv(row,config):
    date_str = row['date'].split(" ")[0]
    start_date = datetime(int(date_str.split("-")[0]),int(date_str.split("-")[1]),int(date_str.split("-")[2]),int(row['hour']),0,0)
    if row['strategy'] in ONED_STRATEGIES:
        days_back = 1
    # elif row['strategy'] in THREED_STRATEGIES:
    #     days_back = 3
    end_date = backtrader_helper.create_end_date(start_date, days_back)
    trading_aggregates, option_symbols = backtrader_helper.create_options_aggs_inv(row,start_date,end_date=end_date,spread_length=config['spread_length'],config=config)
    volume_data = backtrader_helper.create_volume_aggs_inv(row,start_date=None,end_date=start_date,options=option_symbols,config=config)
    return start_date, end_date, row['symbol'], row['alert_price'], row['strategy'], option_symbols, trading_aggregates, volume_data

def create_simulation_data_pt(row,config):
    print(row)
    date_str = row['recent_date'].split(" ")[0]
    start_date = datetime(int(date_str.split("-")[0]),int(date_str.split("-")[1]),int(date_str.split("-")[2]),int(row['hour']),0,0)
    if row['strategy'] in ONED_STRATEGIES:
        days_back = 1
    # elif row['strategy'] in THREED_STRATEGIES:
    #     days_back = 3
    end_date = backtrader_helper.create_end_date(start_date, days_back)
    trading_aggregates, option_symbols, open_price = backtrader_helper.create_options_aggs_pt(row,start_date,end_date=end_date,config=config)
    return start_date, end_date, row['symbol'], open_price, row['strategy'], option_symbols, trading_aggregates

def buy_iterate_sellV2_invalerts(symbol, option_symbol, open_prices, strategy, polygon_df, position_id, trading_date, alert_hour,order_id,config,row,order_num,quantity):

    open_price = open_prices[0]
    open_datetime = datetime(int(trading_date.split("-")[0]),int(trading_date.split("-")[1]),int(trading_date.split("-")[2]),int(alert_hour),0,0,tzinfo=pytz.timezone('US/Eastern'))
    contract_cost = round(open_price * 100,2)

    if strategy in CALL_STRATEGIES:
        contract_type = "calls"
    elif strategy in PUT_STRATEGIES:
        contract_type = "puts"
        
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "quantity": quantity, "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id, "contract_type": contract_type}

    if config['model'] == "CDVOLVARVC2":
        try:
            if strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLVARVC2(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLVARVC2(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLVARVC2")
            print(polygon_df)
            return "NO DICT"
    elif config['model'] == "CDVOLVARVC":
        try:
            if strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLVARVC")
            print(polygon_df)
            return "NO DICT"
    elif config['model'] == "CDVOLVARVC3":
        try:
            if strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLVARVC3(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLVARVC3(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLVARVC3")
            print(polygon_df)
            return "NO DICT"
    elif config['model'] == "CDVOLVARVC_AA1":
        try:
            if strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLVARVC_AA1(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLVARVC_AA1(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLVARVCAA1")
            print(polygon_df)
            return "NO DICT"
    
    
    try:
        sell_dict['position_id'] = position_id
        sell_dict['quantity'] = quantity
        results_dict = backtrader_helper.create_results_dict(buy_dict, sell_dict, order_id)
        results_dict['position_id'] = position_id
        # transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
        buy_dt = buy_dict['open_datetime']
        sell_dt = sell_dict['close_datetime']
        buy_dt = datetime(buy_dt.year,buy_dt.month,buy_dt.day,buy_dt.hour)
        sell_dt = datetime(sell_dt.year,sell_dt.month,sell_dt.day,sell_dt.hour)
        if buy_dt > sell_dt:
            print(f"Date Mismatch for {symbol}")
            print(f"{buy_dt} vs. {sell_dt}")
            # print(sell_dict['close_datetime'])
            print()
    except Exception as e:
        print(f"Error {e} in transaction_dict for {symbol}")
        print(buy_dict)
        print(sell_dict)
        print(results_dict)
        print()
    return results_dict

def buy_iterate_sellV2_invalerts_pt(symbol, option_symbol, open_prices, strategy, polygon_df, position_id, trading_date, alert_hour,order_id,config,row,order_num,quantity):
    open_price = open_prices[0]
    open_datetime = datetime(int(trading_date.split("-")[0]),int(trading_date.split("-")[1]),int(trading_date.split("-")[2]),int(alert_hour),0,0,tzinfo=pytz.timezone('US/Eastern'))
    contract_cost = round(open_price * 100,2)

    if strategy in CALL_STRATEGIES:
        contract_type = "calls"
    elif strategy in PUT_STRATEGIES:
        contract_type = "puts"
        
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "quantity": quantity, "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id, "contract_type": contract_type}

    # if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
    #     sell_dict = trade.tda_CALL_3D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num)
    # elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
    #     sell_dict = trade.tda_PUT_3D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num)
    if strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
        sell_dict = trade.tda_CALL_1D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num)
    elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
        sell_dict = trade.tda_PUT_1D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=-row['target_pct'],vol=float(row["target_pct"]),order_num=order_num)

    
    try:
        sell_dict['position_id'] = position_id
        results_dict = backtrader_helper.create_results_dict(buy_dict, sell_dict, order_id)
        results_dict['position_id'] = position_id
        # transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
        buy_dt = buy_dict['open_datetime']
        sell_dt = sell_dict['close_datetime']
        buy_dt = datetime(buy_dt.year,buy_dt.month,buy_dt.day,buy_dt.hour)
        sell_dt = datetime(sell_dt.year,sell_dt.month,sell_dt.day,sell_dt.hour)
        if buy_dt > sell_dt:
            print(f"Date Mismatch for {symbol}")
            print(f"{buy_dt} vs. {sell_dt}")
            # print(sell_dict['close_datetime'])
            print()
    except Exception as e:
        print(f"Error {e} in transaction_dict for {symbol}")
        print(buy_dict)
        print(sell_dict)
        print(results_dict)
        print()
    return results_dict

def simulate_trades_invalerts_pt(data,config):
    positions_list = []
    for i, row in data.iterrows():
        order_num = 1
        ## These variables are crucial for controlling the buy/sell flow of the simulation.
        alert_hour = row['hour']
        trading_date = row['recent_date']
        trading_date = trading_date.split(" ")[0]
        start_date, end_date, symbol, mkt_price, strategy, option_symbols, enriched_options_aggregates = create_simulation_data_pt(row,config)
        order_dt = start_date.strftime("%m+%d")
        pos_dt = start_date.strftime("%Y-%m-%d-%H")
        position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{pos_dt}"
        open_trade_dt = start_date.strftime('%Y-%m-%d %H:%M')
        results = []

        for df in enriched_options_aggregates:
            # try:
            open_prices = df['o'].values
            ticker = df.iloc[0]['ticker']
            order_id = f"{order_num}_{order_dt}"
            quantity = df.iloc[0]['quantity']
            results_dict = buy_iterate_sellV2_invalerts_pt(symbol, ticker, open_prices, strategy, df, position_id, trading_date, alert_hour, order_id,config,row,order_num,quantity=quantity)
            if results_dict == "NO DICT":
                continue
            results_dict['order_num'] = order_num
            print(f"results_dict for {symbol} and {ticker}")
            print(results_dict)
            print()
            if len(results_dict) == 0:
                print(f"Error in simulate_trades_invalerts for {symbol} and {ticker}")
                print(f"{order_id}_{order_dt}")
                continue

            results.append(results_dict)
            order_num += 1
            # except Exception as e:
            #     print(f"error: {e} in simulate_trades_invalerts_pt")
            #     print(df)
            #     continue
        
        try:
            position_trades = {"position_id": position_id, "transactions": results, "open_datetime": open_trade_dt}
        except Exception as e:
            print(f"Error in position_trades for {position_id} {e}")
            print(results)
            print()
            continue
        positions_list.append(position_trades)
    
    return positions_list

# def simulate_trades_invalerts(data,config):
#     positions_list = []
#     for i, row in data.iterrows():
#         order_num = 1
#         ## These variables are crucial for controlling the buy/sell flow of the simulation.
#         alert_hour = row['hour']
#         trading_date = row['date']
#         trading_date = trading_date.split(" ")[0]
#         start_date, end_date, symbol, mkt_price, strategy, option_symbols, enriched_options_aggregates = create_simulation_data_inv(row,config)
#         order_dt = start_date.strftime("%m+%d")
#         pos_dt = start_date.strftime("%Y-%m-%d-%H")
#         position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{pos_dt}"
#         open_trade_dt = start_date.strftime('%Y-%m-%d %H:%M')
#         results = []

#         for df in enriched_options_aggregates:
#             try:
#                 open_prices = df['o'].values
#                 ticker = df.iloc[0]['ticker']
#                 order_id = f"{order_num}_{order_dt}"
#                 results_dict = buy_iterate_sellV2_invalerts(symbol, ticker, open_prices, strategy, df, position_id, trading_date, alert_hour, order_id,config,row,order_num)
#                 if results_dict == "NO DICT":
#                     continue
#                 results_dict['order_num'] = order_num
#                 print(f"results_dict for {symbol} and {ticker}")
#                 print(results_dict)
#                 print()
#                 if len(results_dict) == 0:
#                     print(f"Error in simulate_trades_invalerts for {symbol} and {ticker}")
#                     print(f"{order_id}_{order_dt}")
#                     continue

#                 results.append(results_dict)
#                 order_num += 1
#             except Exception as e:
#                 print(f"error: {e} in buy_iterate_sellV2_invalerts HERE")
#                 print(df)
#                 continue
        
#         try:
#             position_trades = {"position_id": position_id, "transactions": results, "open_datetime": open_trade_dt}
#         except Exception as e:
#             print(f"Error in position_trades for {position_id} {e}")
#             print(results)
#             print()
#             continue
#         positions_list.append(position_trades)
    
#     return positions_list

def simulate_trades_invalert_v2(row,config,portfolio_cash):
    ## These variables are crucial for controlling the buy/sell flow of the simulation.
    order_num = 1
    alert_hour = row['hour']
    trading_date = row['date']
    symbol = row['symbol']
    trading_date = trading_date.split(" ")[0]
    start_date, end_date, symbol, mkt_price, strategy, option_symbols, enriched_options_aggregates, volume_data = create_simulation_data_inv(row,config)
    order_dt = start_date.strftime("%m+%d")
    pos_dt = start_date.strftime("%Y-%m-%d-%H")
    position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{pos_dt}"
    open_trade_dt = start_date.strftime('%Y-%m-%d %H:%M')
    results = []

    contract_price_info = {}
    for contract in enriched_options_aggregates:
        try:
            contract_price_info[contract] = {
                "open_price":enriched_options_aggregates[contract]['o'][0],
                "volume_15EMA": volume_data[contract],
            }
        except Exception as e:
            print(f"Error in contract_price_info for {contract} {e}")
            continue
        
    contract_df = bet_sizer(contract_price_info, config['risk_unit'],portfolio_cash,config,start_date,symbol)
    if len(contract_df) == 0:
        print(f"Error in simulate_trades_invalert_v2 for {symbol}")
        return []
    contracts = contract_df.to_dict('records')
    ### Now we need to take that contract sizing info and pass it through to the buy_iterate_sellV2_invalerts function
    ## This way we can actually no the position of the trade and not have it predetermined.

    for contract in contracts:
        option_aggs = enriched_options_aggregates[contract['option_symbol']]
        try:
            open_prices = option_aggs['o'].values
            ticker = option_aggs.iloc[0]['ticker']
            order_id = f"{order_num}_{order_dt}"
            results_dict = buy_iterate_sellV2_invalerts(
                symbol, ticker, open_prices, strategy, option_aggs, position_id, trading_date, alert_hour, 
                order_id,config,row,order_num=contract['spread_position'],quantity=contract['quantity']
                )
            if results_dict == "NO DICT":
                continue
            results_dict['order_num'] = contract['spread_position']
            results_dict['quantity'] = contract['quantity']
            print(f"results_dict for {symbol} and {ticker}")
            print(results_dict)
            print()
            if len(results_dict) == 0:
                print(f"Error in simulate_trades_invalerts for {symbol} and {ticker}")
                print(f"{order_id}_{order_dt}")
                continue

            results.append(results_dict)
            order_num += 1
        except Exception as e:
            print(f"error: {e} in buy_iterate_sellV2_invalerts HERE22")
            print(option_aggs)
            continue
    
    try:
        position = {"position_id": position_id, "transactions": results, "open_datetime": open_trade_dt}
    except Exception as e:
        print(f"Error in position_trades for {position_id} {e}")
        print(results)
        print()
        return []
    
    return position
    # return purchases_list, sales_list, order_results_list, positions_list


### BET SIZING FUNCTIONS ###
# def build_trade(position, risk_unit,put_adjustment,portfolio_cash,config):
#     print("BUILDING TRADE")
#     print(position)
#     buy_orders = []
#     sell_orders = []
#     contract_costs = []
#     # spread_start, spread_end = config['spread_search'].split(":")
#     # transactions = position['transactions'][int(spread_start):int(spread_end)]
#     transactions = simulate_trades_invalert_SINGLE(position,config,portfolio_cash)
#     for transaction in position['transactions']:
#         # print(type(trade_info))
#         # print(position_id)
#         # print(trade_info[0])
#         try:
#             transaction['sell_info']['close_trade_dt'] = transaction['close_trade_dt']
#             buy_orders.append(transaction['buy_info'])
#             sell_orders.append(transaction['sell_info'])
#             contract_costs.append({"option_symbol":transaction['buy_info']['option_symbol'],"contract_cost":transaction['buy_info']['contract_cost']})
#         except Exception as e:
#             print(f"ERROR in build_trade f{e}")
#             print(e)
#             print(transaction)
#             print(position)
#             return [], []

#     quantities = bet_sizer(contract_costs, buy_orders, sell_orders, risk_unit,portfolio_cash,config,position['open_datetime'])
#     if quantities == None:
#         # sized_buys,sized_sells = add_extra_contracts(position['transactions'][int(spread_end):],risk_unit,portfolio_cash,config)
#         print("ERROR in build_trade, no trades")
#         print(position['position_id'])
#         return [], []
#         # if sized_buys == None:
#         # else:
#         #     return [sized_buys], [sized_sells]
    
#     return sized_buys, sized_sells

def build_trade(position, risk_unit,put_adjustment,portfolio_cash,config):
    buy_orders = []
    sell_orders = []
    contract_costs = []
    # spread_start, spread_end = config['spread_search'].split(":")
    # transactions = position['transactions'][int(spread_start):int(spread_end)]
    simulated_position = simulate_trades_invalert_v2(position,config,portfolio_cash)
    if len(simulated_position) == 0:
        return []
    for transaction in simulated_position['transactions']:
        try:
            transaction['sell_info']['close_trade_dt'] = transaction['sell_info']['close_datetime'].strftime('%Y-%m-%d %H:%M')
        # buy_orders.append(transaction['buy_info'])
        # sell_orders.append(transaction['sell_info'])
        # contract_costs.append({"option_symbol":transaction['buy_info']['option_symbol'],"contract_cost":transaction['buy_info']['contract_cost']})
        except Exception as e:
            print(f"ERROR in build_trade f{e}")
            print(e)
            print(transaction)
            print(position)
            return []

    
    return simulated_position

def bet_sizer(contracts_cost_volume,risk_unit,portfolio_cash,config,open_datetime,symbol):
    # if config['scale'] == "FIX":
        ## we pass in predetermined portfolio amount from simulator
    target_cost = portfolio_cash
    # else:
    #     target_cost = (risk_unit * portfolio_cash)

    try:
        # if config['spread_type'] == "standard":
        quantities = size_spread_quantities(contracts_cost_volume, target_cost, config,open_datetime,symbol)
        # elif config['spread_type'] == "proxy":
        #     quantities = create_single_contract_spread_proxy(contract_costs, target_cost, config,open_datetime)
    except Exception as e:
        print(f"Error in bet_sizer {e}")
        return None
    if len(quantities) == 0:
        return []
    return quantities

# def bet_sizer(contract_costs,risk_unit,portfolio_cash,config,open_datetime):
#     ## FUNDS ADJUSTMENT
#     available_funds = portfolio_cash
#     ## PUT ADJUSTMENT
#     target_cost = (risk_unit * available_funds)

#     if config['spread_type'] == "standard":
#         quantities = size_spread_quantities(contract_costs, target_cost, config,open_datetime)
#     elif config['spread_type'] == "proxy":
#         quantities = create_single_contract_spread_proxy(contract_costs, target_cost, config,open_datetime)
#     # quantities = finalize_trade(buy_orders, spread_cost, target_cost)
#     # buy_df = pd.DataFrame.from_dict(buy_orders)
#     # sell_df = pd.DataFrame.from_dict(sell_orders)
#     # buy_df['quantity'] = 0
#     # sell_df['quantity'] = 0


#     # if len(quantities) == 0:
#     #     return None, None
#     # for index, row in quantities.iterrows():
#     #     try:
#     #         if row['quantity'] == 0:
#     #             continue
#     #         else:
#     #             try:
#     #                 buy_df.loc[buy_df['option_symbol'] == row['option_symbol'], 'quantity'] = row['quantity']
#     #                 sell_df.loc[sell_df['option_symbol'] == row['option_symbol'], 'quantity'] = row['quantity']
#     #             except Exception as e:
#     #                 print(f"Error {e} in size_trade {row}")
#     #                 print(e)
#     #                 print(buy_df)
#     #                 print(sell_df)
#     #                 print()
#     #                 return [], []
#     #     except Exception as e:
#     #         print("size_trade 2")
#     #         print(e)
#     #         print(buy_orders)
#     #         return [], []

#     # buy_df = buy_df.loc[buy_df['quantity'] > 0]
#     # sell_df = sell_df.loc[sell_df['quantity'] > 0]
#     # buy_orders = buy_df.to_dict('records')
#     # sell_orders = sell_df.to_dict('records')
#     return quantities

def add_extra_contracts(positions, risk_unit,portfolio_cash,config):
    target_cost = (risk_unit * portfolio_cash)
    for position in positions:
        print(position)
        if position['buy_info']['contract_cost'] <= target_cost:
            position['buy_info']['quantity'] = 1
            position['sell_info']['quantity'] = 1
            position['sell_info']['close_trade_dt'] = position['close_trade_dt']
            return position['buy_info'], position['sell_info']
        else:
            continue
    return None, None

def size_spread_quantities(contracts_cost_volume, target_cost, config, open_datetime,symbol):
    # dt = datetime.strptime(open_datetime, "%Y-%m-%d %H:%M")
    day_of_week = open_datetime.weekday()
    spread_start, spread_end = config['spread_search'].split(":")
    spread_length = config['spread_length']
    tickers = list(contracts_cost_volume.keys())
    capital_distributions = config['capital_distributions']
    # print("CONTRACTS")
    # print(contracts_details)
    if symbol in ["SPY","QQQ","IWM"]:
        adjusted_contracts = tickers[int(spread_start)+2:int(spread_end)+2]
        capital_distributions = [float(x) for x in capital_distributions.split(",")]
    elif spread_length == 2:
        if day_of_week == 3:
            adjusted_contracts = tickers[int(spread_start):int(spread_end)]
            capital_distributions = [float(x) for x in capital_distributions.split(",")]
        else:
            adjusted_contracts = tickers[int(spread_start):int(spread_end)]
            capital_distributions = [float(x) for x in capital_distributions.split(",")]
    else:
        # if day_of_week >= 2:
        #     adjusted_contracts = tickers[(int(spread_start)-1):(int(spread_end)-1)]
        #     capital_distributions = [float(x) for x in capital_distributions.split(",")]
        # else:
        adjusted_contracts = tickers[int(spread_start):int(spread_end)]
        capital_distributions = [float(x) for x in capital_distributions.split(",")]

    final_contracts = []
    for contract in adjusted_contracts:
        final_contracts.append({"option_symbol":contract,"contract_cost":(contracts_cost_volume[contract]['open_price']*100),"contract_volume":contracts_cost_volume[contract]['volume_15EMA']})
    
    spread_candidates = configure_contracts_for_trade_pct_based_v3(final_contracts, target_cost, capital_distributions)

    if len(spread_candidates) == 0:
        return []


    details_df = pd.DataFrame(spread_candidates)
    details_df = details_df.loc[details_df['quantity'] > 0]
    details_df.reset_index(drop=True, inplace=True)
    details_df['spread_position'] = details_df.index
    return details_df

def configure_contracts_for_trade(contracts_details, target_cost, spread_length):
    spread_candidates = []
    total_cost = 0
    cost_remaining = target_cost
    for contract in contracts_details:
        if contract['contract_cost'] < cost_remaining:
            spread_candidates.append(contract)
            cost_remaining -= contract['contract_cost']
            total_cost += contract['contract_cost']
        if len(spread_candidates) == spread_length:
            return spread_candidates, total_cost
    return spread_candidates, total_cost

def configure_contracts_for_trade_pct_based(contracts_details, target_cost, spread_length):
    spread_candidates = []
    cost_remaining = target_cost
    split_cost = target_cost / spread_length
    for contract in contracts_details:
        contract_quantity = 0
        split_cost_remaining = split_cost
        while split_cost_remaining > 0:
            if (split_cost_remaining - contract['contract_cost']) > 0:
                split_cost_remaining -= contract['contract_cost']
                contract_quantity += 1
            else:
                break

        spread_candidates.append({"option_symbol": contract['option_symbol'], "quantity": contract_quantity,"contract_cost": contract['contract_cost']})
    return spread_candidates
    
def configure_contracts_for_trade_pct_based_v3(contracts_details, capital, capital_distributions):
    sized_contracts = []
    total_capital = capital
    free_capital = 0
    for index, contract in enumerate(reversed(contracts_details)):
        contract_capital = (capital_distributions[-(index+1)]*total_capital) + free_capital
        quantities = determine_shares(contract, contract_capital)
        if quantities > 0:
            sized_contracts.append({"option_symbol": contract['option_symbol'], "quantity": quantities,"contract_cost": contract['contract_cost']})
            free_capital = contract_capital - (quantities * contract['contract_cost'])
        else:
            free_capital += contract_capital
    return sized_contracts

def configure_contracts_for_trade_pct_based_v2(contracts_details, capital, capital_distributions):
    sized_contracts = []
    total_capital = capital
    free_capital = 0
    for index, contract in enumerate(contracts_details):
        contract_capital = (capital_distributions[index]*total_capital) + free_capital
        quantities = determine_shares(contract, contract_capital)
        if quantities > 0:
            sized_contracts.append({"option_symbol": contract['option_symbol'], "quantity": quantities,"contract_cost": contract['contract_cost']})
            free_capital = contract_capital - (quantities * contract['contract_cost'])
        else:
            free_capital += contract_capital
    return sized_contracts

# def configure_contracts_for_trade_pct_based_proxy_spread(contract, capital, capital_distributions):
#     sized_contracts = []
#     total_capital = capital
#     free_capital = 0
#     for distribution in capital_distributions:
#         contract_capital = (distribution*total_capital) + free_capital
#         quantities = determine_shares(contract['contract_cost'], contract_capital)
#         if quantities > 0:
#             sized_contracts.append({"option_symbol": contract['option_symbol'], "quantity": quantities,"contract_cost": contract['contract_cost']})
#             free_capital = contract_capital - (quantities * contract['contract_cost'])
#         else:
#             free_capital += contract_capital
#     return sized_contracts

def determine_shares(contract_details, target_cost):
    shares = math.floor(target_cost / contract_details['contract_cost'])
    if shares > (contract_details['contract_volume']*.1):
        shares = math.floor((contract_details['contract_volume']*.1))
    return shares

def create_single_contract_spread_proxy(contracts_details, target_cost, config, open_datetime):
    dt = datetime.strptime(open_datetime, "%Y-%m-%d %H:%M")
    day_of_week = dt.weekday()
    spread_start, spread_end = config['spread_search'].split(":")
    adjusted_target_cost = target_cost
    spread_length = config['spread_length']

    capital_distributions = [float(x) for x in config[capital_distributions].split(",")]
    if day_of_week == 3:
        ## selecting first out of the money
        adjusted_contracts = contracts_details[1]
    elif day_of_week == 2:
        ## selecting second out of the money
        adjusted_contracts = contracts_details[2]
    else:
        ## selecting third out of the money
        adjusted_contracts = contracts_details[3]

    spread_candidates = configure_contracts_for_trade_pct_based_v2(adjusted_contracts, adjusted_target_cost, capital_distributions)

    if len(spread_candidates) == 0:
        return []

    details_df = pd.DataFrame(spread_candidates)
    details_df = details_df.loc[details_df['quantity'] > 0]
    details_df.reset_index(drop=True, inplace=True)
    details_df['spread_position'] = details_df.index
    return details_df


def calculate_spread_cost(contracts_details):
    cost = 0
    for contract in contracts_details:
        cost += contract['contract_cost']
    return cost

def build_volume_features(df):
    avg_volume = df['v'].mean()
    avg_transactions = df['n'].mean()
    return avg_volume, avg_transactions

def finalize_trade(contracts_details, spread_cost, target_cost):
    if len(contracts_details) == 1:
        spread_multiplier = math.floor(target_cost/spread_cost)
        return [spread_multiplier]
    elif len(contracts_details) == 2:
        if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
            return [1,1]
        elif spread_cost > (1.1*target_cost):
            spread2_cost = calculate_spread_cost(contracts_details[1:])
            if spread2_cost < (1.1*target_cost):
                return [0,1]
            else:
                contract = contracts_details[0]
                single_contract_cost = 100 * contract['contract_cost']
                if single_contract_cost > (1.1*target_cost):
                    contract = contracts_details[1]
                    single_contract_cost = 100 * contract['contract_cost']
                    if single_contract_cost > (1.1*target_cost):
                        return [0,0]
                else:
                    return [1,0]
        elif spread_cost < (.9*target_cost):
            spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
            if add_one:  
                return [(spread_multiplier+1),spread_multiplier]
            else:
                return [spread_multiplier,spread_multiplier]
        else:
            print("ERROR")
            return [0,0]
    elif len(contracts_details) >= 3:
        contracts_details = contracts_details[0:3]
        if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
            return [1,1,1]
        elif spread_cost > (1.1*target_cost):
            spread2_cost = calculate_spread_cost(contracts_details[1:])
            if spread2_cost < (1.1*target_cost):
                return [0,1,1]
            else:
                contract = contracts_details[0]
                single_contract_cost = 100 * contract['contract_cost']
                if single_contract_cost > (1.1*target_cost):
                    contract = contracts_details[1]
                    single_contract_cost = 100 * contract['contract_cost']
                    if single_contract_cost > (1.1*target_cost):
                        contract = contracts_details[2]
                        single_contract_cost = 100 * contract['contract_cost']
                        if single_contract_cost > (1.1*target_cost):
                                return [0,0,0]
                        else:
                            return [0,0,1]
                    else:
                        return [0,1,0]
                else:
                    return [1,0,0] 
        elif spread_cost < (.9*target_cost):
            spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
            if add_one:  
                return [(spread_multiplier+1),spread_multiplier,spread_multiplier]
            else:
                return [spread_multiplier,spread_multiplier,spread_multiplier]
        else:
            print("ERROR")
            return [0,0,0]
    else:
        return "NO TRADES"
            
def add_spread_cost(spread_cost, target_cost, contracts_details):
    add_one = False
    spread_multiplier = 1
    total_cost = spread_cost
    if spread_cost == 0:
        return 0, False
    else:
        while total_cost <= (1.1*target_cost):
            spread_multiplier += 1
            total_cost = spread_cost * spread_multiplier
        
        if total_cost > (1.1*target_cost):
            spread_multiplier -= 1
            total_cost -= spread_cost

        if total_cost < (.67*target_cost):
            add_one = True

    return spread_multiplier, add_one
    


# def build_trade_analytics_RMF(row, polygon_df, derivative_open_price, index, quantity, sell_code, aggregate_classification, hilo_score):
#     trade_dict = {}
#     before_df = polygon_df.iloc[:index]
#     after_df = polygon_df.iloc[index:]
#     trade_dict['max_value_before'] = before_df['h'].max()
#     trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
#     trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date'].strftime("%Y-%m-%d %H:%M")
#     trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)

#     if len(after_df) > 0:
#         trade_dict['max_value_after'] = after_df['h'].max()
#         trade_dict['max_value_after_idx'] = after_df['h'].idxmax()
#         trade_dict['max_value_after_date'] = after_df.loc[trade_dict['max_value_after_idx']]['date'].strftime("%Y-%m-%d %H:%M")
#         trade_dict['max_value_after_pct_change'] = ((trade_dict['max_value_after'] - derivative_open_price)/derivative_open_price)
#     else:
#         trade_dict['max_value_after'] = None
#         trade_dict['max_value_after_idx'] = None
#         trade_dict['max_value_after_date'] = None
#         trade_dict['max_value_after_pct_change'] = None

#     trade_dict["close_price"] = row['alert_price']
#     trade_dict["close_datetime"] = row['date'].to_pydatetime()
#     trade_dict["quantity"] = quantity
#     trade_dict["contract_cost"] = (row['alert_price']*100)
#     trade_dict["option_symbol"] = row['ticker']
#     trade_dict["sell_code"] = sell_code
#     trade_dict["aggregate_classification"] = aggregate_classification
#     trade_dict["hilo_score"] = hilo_score
#     return trade_dict

        


if __name__ == "__main__":
    file_names = ["2023-01-02","2023-01-09","2023-01-16","2023-01-23","2023-01-30","2023-02-06",
                  "2023-02-13","2023-02-20","2023-02-27","2023-03-06"
                  ,"2023-03-13","2023-03-20","2023-03-27","2023-04-03","2023-04-10","2023-04-17",
                  "2023-04-24","2023-05-01","2023-05-08","2023-05-15","2023-05-22","2023-05-29","2023-06-05"]
    for file_name in file_names:
        s3_to_local(file_name)
