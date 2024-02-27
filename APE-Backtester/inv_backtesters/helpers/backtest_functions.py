from datetime import timedelta, datetime
import datetime as dt
import pandas as pd
from helpers import backtrader_helper
import warnings
import helpers.momentum_strategies as trade
import boto3
import pytz
from helpers.constants import *
from helpers.RMF_strategies import tda_CALL_1D_RMF, tda_PUT_1D_RMF, tda_CALL_3D_RMF, tda_PUT_3D_RMF

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
    elif row['strategy'] in THREED_STRATEGIES:
        days_back = 3
    end_date = backtrader_helper.create_end_date(start_date, days_back)
    trading_aggregates, option_symbols = backtrader_helper.create_options_aggs_inv(row,start_date,end_date=end_date,spread_length=config['spread_length'],config=config)
    return start_date, end_date, row['symbol'], row['o'], row['strategy'], option_symbols, trading_aggregates

def buy_iterate_sellV2_invalerts(symbol, option_symbol, open_prices, strategy, polygon_df, position_id, trading_date, alert_hour,order_id,config,row):
    open_price = open_prices[0]
    open_datetime = datetime(int(trading_date.split("-")[0]),int(trading_date.split("-")[1]),int(trading_date.split("-")[2]),int(alert_hour),0,0,tzinfo=pytz.timezone('US/Eastern'))
    contract_cost = round(open_price * 100,2)

    if strategy in CALL_STRATEGIES:
        contract_type = "calls"
    elif strategy in PUT_STRATEGIES:
        contract_type = "puts"
        
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "quantity": 1, "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id, "contract_type": contract_type}

    if config['model'] == "stdcls":
        try:
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_stdcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["threeD_stddev50"]))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_stdcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["threeD_stddev50"]))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_stdcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["oneD_stddev50"]))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_stdcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["oneD_stddev50"]))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} stdcls")
            print(polygon_df)
            return {}
    elif config['model'] == "VCcls":
        try:
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_VCcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["threeD_stddev50"]))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_VCcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["threeD_stddev50"]))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_VCcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["oneD_stddev50"]))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict= trade.tda_PUT_1D_VCcls(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["oneD_stddev50"]))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} VCcls")
            print(polygon_df)
            return {}
    elif config['model'] == "stdclsAGG":
        try:
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["return_vol_10D"]))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["return_vol_10D"]))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["return_vol_10D"]))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict= trade.tda_PUT_1D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["return_vol_10D"]))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} stdClassAgg")
            print(polygon_df)
            return {}
    elif config['model'] == "simple":
        try:
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_simple(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["threeD_stddev50"]))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_simple(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["threeD_stddev50"]))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_simple(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["oneD_stddev50"]))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict= trade.tda_PUT_1D_simple(polygon_df,open_datetime,1,config,target_pct=row['target_value'],vol=float(row["oneD_stddev50"]))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} stdClassAgg")
            print(polygon_df)
            return {}
    elif config['model'] == "CDVOL":
        try:
 
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(abs(row["target_pct"])))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(abs(row["target_pct"])))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(abs(row["target_pct"])))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=-row['target_pct'],vol=float(abs(row["target_pct"])))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLAGG")
            print(polygon_df)
            return {}
    elif config['model'] == "CDVOLAGG":
        try:
 
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=-row['target_pct'],vol=float(row["return_vol_10D"]))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLAGG")
            print(polygon_df)
            return {}
    elif config['model'] == "CDVOLSFE":
        try:
 
            if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_3D_CDVOLSFE(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_3D_CDVOLSFE(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
                sell_dict = trade.tda_CALL_1D_CDVOLSFE(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
                sell_dict = trade.tda_PUT_1D_CDVOLSFE(polygon_df,open_datetime,1,config,target_pct=-row['target_pct'],vol=float(row["return_vol_10D"]))
        except Exception as e:
            print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLSFE")
            print(polygon_df)
            return {}
        
        # elif config['model'] == "CDVOLSFE":
        # try:
 
        #     if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
        #         sell_dict = trade.tda_CALL_3D_CDVOLSFE(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            # elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
            #     sell_dict = trade.tda_CALL_1D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
            # elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
            #     sell_dict = trade.tda_PUT_3D_CDVOLAGG(polygon_df,open_datetime,1,config,target_pct=row['target_pct'],vol=float(row["return_vol_10D"]))
        #     elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
        #         sell_dict = trade.tda_PUT_1D_CDVOLSFE(polygon_df,open_datetime,1,config,target_pct=-row['target_pct'],vol=float(row["return_vol_10D"]))
        # except Exception as e:
        #     print(f"Error {e} in sell_dict for {symbol} in {strategy} CDVOLSFE")
        #     print(polygon_df)
        #     return {}
    # elif config['model'] == "RMF":
    #     # try:
    #         if strategy in THREED_STRATEGIES and strategy in CALL_STRATEGIES:
    #             sell_dict = trade.tda_CALL_3D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['rm_target_pct'],vol=float(row["threeD_stddev50"]))
    #         elif strategy in THREED_STRATEGIES and strategy in PUT_STRATEGIES:
    #             sell_dict = trade.tda_PUT_3D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['rm_target_pct'],vol=float(row["threeD_stddev50"]))
    #         elif strategy in ONED_STRATEGIES and strategy in CALL_STRATEGIES:
    #             sell_dict = trade.tda_CALL_1D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['rm_target_pct'],vol=float(row["oneD_stddev50"]))
    #         elif strategy in ONED_STRATEGIES and strategy in PUT_STRATEGIES:
    #             sell_dict= trade.tda_PUT_1D_stdclsAGG(polygon_df,open_datetime,1,config,target_pct=row['rm_target_pct'],vol=float(row["oneD_stddev50"]))
        # except Exception as e:
        #     print(f"Error {e} in sell_dict for {symbol} in {strategy} stdClassAgg")
        #     print(polygon_df)
        #     return {}
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
    # return buy_dict, sell_dict, results_dict, transaction_dict, open_datetime

def simulate_trades_invalerts(data,config):
    positions_list = []
    order_num = 1
    for i, row in data.iterrows():
        ## These variables are crucial for controlling the buy/sell flow of the simulation.
        alert_hour = row['hour']
        trading_date = row['date']
        trading_date = trading_date.split(" ")[0]
        start_date, end_date, symbol, mkt_price, strategy, option_symbols, enriched_options_aggregates = create_simulation_data_inv(row,config)
        order_dt = start_date.strftime("%m+%d")
        pos_dt = start_date.strftime("%Y-%m-%d-%H")
        position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{pos_dt}"

        results = []
        for df in enriched_options_aggregates:
            try:
                open_prices = df['o'].values
                ticker = df.iloc[0]['ticker']
                order_id = f"{order_num}_{order_dt}"
                results_dict = buy_iterate_sellV2_invalerts(symbol, ticker, open_prices, strategy, df, position_id, trading_date, alert_hour, order_id,config,row)
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
                print(f"error: {e} in buy_iterate_sellV2_invalerts")
                print(df)
                continue
        
        try:
            position_trades = {"position_id": position_id, "transactions": results, "open_datetime": results_dict['open_trade_dt']}
            # print("TEST")
            # print(position_trades)
        except Exception as e:
            print(f"Error in position_trades for {position_id}")
            print(results)
            print()
            continue
        positions_list.append(position_trades)
    
    return positions_list
    # return purchases_list, sales_list, order_results_list, positions_list

        


if __name__ == "__main__":
    file_names = ["2023-01-02","2023-01-09","2023-01-16","2023-01-23","2023-01-30","2023-02-06",
                  "2023-02-13","2023-02-20","2023-02-27","2023-03-06"
                  ,"2023-03-13","2023-03-20","2023-03-27","2023-04-03","2023-04-10","2023-04-17",
                  "2023-04-24","2023-05-01","2023-05-08","2023-05-15","2023-05-22","2023-05-29","2023-06-05"]
    for file_name in file_names:
        s3_to_local(file_name)
