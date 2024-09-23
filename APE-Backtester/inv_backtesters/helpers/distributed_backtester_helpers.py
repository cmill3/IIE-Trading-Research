from helpers.constants import *
from datetime import datetime, timedelta
import helpers.momentum_strategies as trade
import helpers.backtrader_helper as backtrader_helper
import helpers.polygon_helper as ph
import pandas as pd
import ast
import pytz

def build_trade(week_df,config):
    week_postions = []
    for _, row in week_df.iterrows():
        try:
            position = simulate_trades(row,config)
            if len(position) == 0:
                continue
            print(f"Position for {row['symbol']} and {row['strategy']}")
        except Exception as e:
            print(f"Error in build_trade for {row['symbol']} and {row['strategy']} {e}")
            continue
        week_postions.append(position)
    return week_postions

def simulate_trades(row,config):
    ## These variables are crucial for controlling the buy/sell flow of the simulation.
    order_num = 1
    alert_hour = row['hour']
    trading_date = row['date']
    symbol = row['symbol']
    trading_date = trading_date.split(" ")[0]
    start_date, end_date, contracts, enriched_options_aggregates, volume_data = create_simulation_data(row,config)
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
        
    # contracts = contract_df.to_dict('records')
    ### Now we need to take that contract sizing info and pass it through to the buy_iterate_sellV2_invalerts function
    ## This way we can actually no the position of the trade and not have it predetermined.
    contracts = contracts[0:5]
    order_num = 0
    for contract in contracts:
        contract['spread_position'] = order_num
        try:
            option_aggs = enriched_options_aggregates[contract['contract_symbol']]
            open_prices = option_aggs['o'].values
            ticker = option_aggs.iloc[0]['ticker']
            order_id = f"{order_num}_{order_dt}"
            results_dict = buy_iterate_sell(
                symbol, ticker, open_prices, row['strategy'], option_aggs, position_id, trading_date, alert_hour, 
                order_id,config,row,order_num=contract['spread_position']
                )
            if results_dict == "NO DICT":
                continue
            results_dict['order_num'] = contract['spread_position']
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

def buy_iterate_sell(symbol, option_symbol, open_prices, strategy, polygon_df, position_id, trading_date, alert_hour,order_id,config,row,order_num):

    open_price = open_prices[0]
    open_datetime = datetime(int(trading_date.split("-")[0]),int(trading_date.split("-")[1]),int(trading_date.split("-")[2]),int(alert_hour),0,0,tzinfo=pytz.timezone('US/Eastern'))
    contract_cost = round(open_price * 100,2)

    if strategy in CALL_STRATEGIES:
        contract_type = "calls"
    elif strategy in PUT_STRATEGIES:
        contract_type = "puts"
        
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id, "contract_type": contract_type}

    if config['model'] == "CDVOLVARVC":
        try:
            if strategy in TREND_STRATEGIES_2H and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_2H_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in TREND_STRATEGIES_2H and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_2H_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in TREND_STRATEGIES_3D and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
            elif strategy in TREND_STRATEGIES_3D and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_CDVOLVARVC(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["target_pct"]),order_num=order_num,symbol=symbol)
        except Exception as e:
            print(f"Error {e} in trading strat for {symbol} in {strategy} CDVOLVARVC2")
            print(polygon_df)
            return "NO DICT"
    
    
    try:
        sell_dict['position_id'] = position_id
        results_dict = backtrader_helper.create_results_dict(buy_dict, sell_dict, order_id)
        results_dict['position_id'] = position_id
        # transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
        buy_dt = datetime.strptime(buy_dict['open_datetime'], "%Y-%m-%d %H:%M")
        sell_dt = datetime.strptime(sell_dict['close_datetime'], "%Y-%m-%d %H:%M")
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

def create_simulation_data(row,config):
    date_str = row['date'].split(" ")[0]
    start_date = datetime(int(date_str.split("-")[0]),int(date_str.split("-")[1]),int(date_str.split("-")[2]),int(row['hour']),0,0)
    if row['strategy'] in TREND_STRATEGIES_2H:
        time_horizon_hours = 2
    elif row['strategy'] in TREND_STRATEGIES_3D:
        time_horizon_hours = 72
    trading_aggregates, option_symbols = create_options_aggs(row,start_date,time_horizon_hours,spread_length=config['spread_length'],config=config)
    volume_data = create_volume_aggs(row,start_date=None,end_date=start_date,options=option_symbols,config=config)
    print(f"Volume Data for {row['symbol']} and {row['strategy']}")
    return start_date, time_horizon_hours,option_symbols, trading_aggregates, volume_data


    
def create_options_aggs(row,start_date,time_horizon_hours,spread_length,config):
    options = []
    enriched_options_aggregates = {}
    expiries = ast.literal_eval(row['expiries'])
    end_date = start_date + timedelta(hours=time_horizon_hours)

    if row['symbol'] in ['SPY','IWM','QQQ']:
        expiry = expiries[1]
    else:
        if config['aa'] == 0:
            expiry = expiries[0]
        elif config['aa'] == 1:
            expiry = expiries[1]
    
    strike = row['symbol'] + expiry
    try:
        underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date,10)
    except Exception as e:
        print(f"Error: {e} in underlying agg for {row['symbol']} of {row['strategy']}")
        try:
            underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date,10)
        except Exception as e:
            print(f"Error: {e} in underlying agg for 2ND {row['symbol']} of {row['strategy']}")
            try: 
                underlying_agg_data = ph.polygon_stockdata_inv(row['symbol'],start_date,end_date,10)
            except Exception as e:
                print(f"Error: {e} in underlying agg for FINAL {row['symbol']} of {row['strategy']}")
                return [], []
            
    try:
        underlying_agg_data.rename(columns={'o':'underlying_price'}, inplace=True)
        open_price = underlying_agg_data['underlying_price'].values[0]
        contracts = ast.literal_eval(row['contracts'])
    except Exception as e:
        print(f"Error: {e} in evaluating contracts for {row['symbol']} of {row['strategy']}")
        print(row['contracts'])
        print(row['symbol'])
        print(start_date)
        print(end_date)
        print(underlying_agg_data)
        print("ANALYSIS NO UNDERLYING")
        print()
        return [], []
    filtered_contracts = [k for k in contracts if strike in k]
    if len(filtered_contracts) == 0:
        print(f"No contracts for {row['symbol']} of {row['strategy']}")
        try:
            strike2 = row['symbol'] + contracts[0][-15:-9]
            filtered_contracts = [k for k in contracts if strike2 in k]
            if len(filtered_contracts) == 0:
                print(f"No contracts 2nd try for {row['symbol']} of {row['strategy']}")
                print(contracts)
                print(strike)
                print(start_date)
                return [], []
        except Exception as e:
            print(f"Error: {e} in 2nd try for {row['symbol']} of {row['strategy']}")
            print(contracts)
            print(strike)
            print(start_date)
            return [], []
    options_df = build_options_df(filtered_contracts, row)
    ## SPREAD ADJUSTMENT
    # options_df = options_df.iloc[config['spread_adjustment']:]
    # spread_start, spread_end = config['spread_search'].split(':')
    # options_df = options_df.iloc[int(spread_start):int(spread_end)]
    for index,contract in options_df.iterrows():
        try:
            options_agg_data = ph.polygon_optiondata(contract['contract_symbol'], start_date, end_date,10)
            enriched_df = pd.merge(options_agg_data, underlying_agg_data[['date', 'underlying_price']], on='date', how='left')
            enriched_df.dropna(inplace=True)
            enriched_options_aggregates[contract['contract_symbol']] = enriched_df
            options.append(contract)
            if len(options) > (spread_length):
                break
        except Exception as e:
            print(f"Error: {e} in options agg for {row['symbol']} of {row['strategy']}")
            print(contract)
            continue
    return enriched_options_aggregates, options

def create_volume_aggs(row,start_date,end_date,options,config):
    volume_aggregates = {}
    for contract in options:
        try:
            volume_agg_data = ph.polygon_optiondata(contract['contract_symbol'], start_date, end_date,10)
            volume_agg_data['volume_15ema'] = volume_agg_data['v'].ewm(span=15, adjust=False).mean()
            volume_aggregates[contract['contract_symbol']] = volume_agg_data['volume_15ema'].values[-1]
        except Exception as e:
            print(f"Error: {e} in volume agg for {row['symbol']} of {row['strategy']}")
            print(contract)
            continue
    print(volume_aggregates)
    return volume_aggregates

def build_options_df(contracts, row):
    if row['symbol'] in ["GOOG","GOOGL","NVDA","AMZN","TSLA"]:
        last_price = ph.get_last_price(row)
        row['alert_price'] = last_price
    df = pd.DataFrame(contracts, columns=['contract_symbol'])
    df['underlying_symbol'] = row['symbol']
    df['option_type'] = row['side']
    try:
        df['strike'] = df.apply(lambda x: extract_strike(x),axis=1)
    except Exception as e:
        print(f"Error: {e} building options df for {row['symbol']}")
        print(df)
        print(contracts)
        return df

    df['strike_diff'] = abs((df['strike'] - row['alert_price'])/row['alert_price'])
    if row['side'] == "P":
        df['in_money'] = df['strike']  > row['alert_price']
        df_in = df.loc[df['strike'] > row['alert_price']]
        df_out = df.loc[df['strike'] < row['alert_price']]
        last_in = df_in.head(1)
        # Concatenate the True rows and the last False row
        result = pd.concat([last_in, df_out])
        result = result.sort_values('strike', ascending=False)
        # df  = df.loc[df['strike_diff'] < 0.075].reset_index(drop=True)
        # print(df)
        # breakkk
    elif row['side'] == "C":
        df['in_money'] = df['strike']  < row['alert_price']
        # Select all rows where 'condition' is True
        df_in = df.loc[df['strike'] < row['alert_price']]
        df_out = df.loc[df['strike'] > row['alert_price']]
        last_in = df_in.tail(1)
        # Concatenate the True rows and the last False row
        result = pd.concat([last_in, df_out])
        result = result.sort_values('strike', ascending=True)

    
    return result

def extract_strike(row):
    str = row['contract_symbol'].split(f"O:{row['underlying_symbol']}")[1]
    if row['option_type'] == 'P':
        str = str.split('P')[1]
    elif row['option_type'] == 'C':
        str = str.split('C')[1]
    strike = str[:-3]
    for i in range(len(strike)):
        if strike[i] == '0':
            continue
        else:
            return int(strike[i:])
        
    return 0