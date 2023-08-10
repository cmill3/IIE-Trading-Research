from datetime import timedelta, datetime
import datetime as dt
import pandas as pd
from helpers import backtrader_helper
import warnings
import helpers.trading_strategies as trade
import boto3

s3 = boto3.client('s3')

def pull_data(s3link):
    # starting_value = backtrader_helper.startbacktrader(1000000)
    # commission_cost = 0.35
    raw_data = backtrader_helper.s3_data(s3link['bucketname'], s3link['objectkey'])
    dataset = pd.DataFrame(raw_data)
    full_data = backtrader_helper.pull_modeling_results(dataset)
    data = full_data[full_data.volume >= 20000000]
    data = data.loc[data['classifier_prediction'] > .5]
    data = data.loc[data['symbol'] != "NU"]
    data.reset_index(inplace=True)
    # start_time = datetime.strptime(raw_data['date'].values[0], '%Y-%m-%d %H:%M:%S')
    end_date = backtrader_helper.create_end_date(raw_data['date'].values[0], 4)
    datetime_list, datetime_index, results = backtrader_helper.create_datetime_index(raw_data['date'].values[0], end_date)
    return data, datetime_list


def pull_data_invalerts(bucket_name, object_key, file_name, prefixes):
    dfs = []
    # prefixes = ["gainers","gainersP","losers","losersC","ma","maP","vdiffC","vdiffP"]
    # prefixes = ["gainers55pct","losers55pct","ma","maP","vdiff_gainC","vdiff_gainP"]
    leveraged_etfs = ["TQQQ","SQQQ","SPXS","SPXL","SOXL","SOXS"]
    # if prefixes == []:
    #     try:
    #         print(f"{object_key}/{file_name}")
    #         obj = s3.get_object(Bucket=bucket_name, Key=f"{object_key}/{file_name}")
    #         df = pd.read_csv(obj.get("Body"))
    #         dfs.append(df)
    #     except:
    #         # print(f"no file for {prefix}")
    #         pass
    # else:
    for prefix in prefixes:
        if prefix == "vdiff_gainC":
            print(f"{object_key}/{prefix}/{file_name}")
            obj = s3.get_object(Bucket=bucket_name, Key=f"{object_key}/{file_name}")
            df = pd.read_csv(obj.get("Body"))
            df['strategy'] = prefix
            dfs.append(df)
        else:
            try:
                print(f"{object_key}/{prefix}/{file_name}")
                obj = s3.get_object(Bucket=bucket_name, Key=f"{object_key}/{prefix}/{file_name}")
                df = pd.read_csv(obj.get("Body"))
                df['strategy'] = prefix
                dfs.append(df)
            except:
                print(f"no file for {prefix}")
                continue
    full_data = pd.concat(dfs)
    filter = full_data[~full_data['symbol'].isin(leveraged_etfs)]
    data = filter[filter.predictions == 1]
    start_time = datetime.strptime(data['date'].values[0], '%Y-%m-%d %H:%M:%S')
    end_date = backtrader_helper.create_end_date(data['date'].values[-1], 4)
    datetime_list, datetime_index, results = backtrader_helper.create_datetime_index(start_time, end_date)
    return data, datetime_list


def create_simulation_data(row):
    # start_date = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
    end_date = backtrader_helper.create_end_date_tstamp(row['date'], 3)
    symbol = row['symbol']
    mkt_price = row['regularMarketPrice']
    contracts = row['contracts']
    strategy = row['title']
    # option_symbol, polygon_dfs = backtrader_helper.data_pull(symbol, start_date, end_date, mkt_price, strategy, contracts)
    trading_aggregates, option_symbols = backtrader_helper.create_options_aggs(symbol, row['date'], end_date, mkt_price, strategy, contracts, spread_length=3)
    return row['date'], end_date, symbol, mkt_price, strategy, option_symbols, trading_aggregates

def create_simulation_data_inv(row):
    date_str = row['date'].split(" ")[0]
    start_date = datetime(int(date_str.split("-")[0]),int(date_str.split("-")[1]),int(date_str.split("-")[2]),int(row['hour']),0,0)
    end_date = backtrader_helper.create_end_date(start_date.strftime('%Y-%m-%d %H:%M:%S'), 4)
    # option_symbol, polygon_dfs = backtrader_helper.data_pull(symbol, start_date, end_date, mkt_price, strategy, contracts)
    trading_aggregates, option_symbols = backtrader_helper.create_options_aggs_inv(row['symbol'],row['contracts'],start_date,end_date=end_date,market_price=row['o'],strategy=row['strategy'],spread_length=3)
    return start_date, end_date, row['symbol'], row['o'], row['strategy'], option_symbols, trading_aggregates

def buy_iterate_sellV2(symbol, option_symbol, open_prices, strategy, polygon_df, position_id):
    open_price = open_prices[0]
    open_dt = polygon_df['date'][0]
    open_datetime = open_dt.to_pydatetime()
    # open_dt_hrmin = open_datetime.strftime("%Y-%m-%d %H:%M")
    # transaction_cost = commission_cost * contract_number
    # total_transaction_cost = transaction_cost * 2
    contract_cost = round(open_price * 100, 2)
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime.strftime("%Y-%m-%dT%H:%M"), "quantity": 1, "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id}

    # if strategy == "day_gainers":
    #     sell_dict = time_decay_alpha_gainers_v0(polygon_df.iloc[1:],open_datetime,quantity)
    # elif strategy == "day_losers":
    #     sell_dict = time_decay_alpha_losers_v0(polygon_df.iloc[1:],open_datetime,quantity)
    # elif strategy == "maP":
    #     sell_dict = time_decay_alpha_maP_v0(polygon_df.iloc[1:],open_datetime,quantity)
    # elif strategy == "most_actives":
    #     sell_dict = time_decay_alpha_ma_v0(polygon_df.iloc[1:],open_datetime,quantity)
    try:
        if strategy == "day_gainers":
            sell_dict = trade.time_decay_alpha_gainers_v0(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "day_losers":
            sell_dict = trade.time_decay_alpha_losers_v0(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "maP":
            sell_dict = trade.time_decay_alpha_maP_v0(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "most_actives":
            sell_dict = trade.time_decay_alpha_ma_v0(polygon_df.iloc[1:],open_datetime,1)
    except Exception as e:
        print(e)
        print("Error in sell_dict")
        print(symbol)
        return {}, {}, {}, {}, "error"
    
    sell_dict['position_id'] = position_id
    try:
        results_dict = backtrader_helper.create_results_dict(buy_dict, sell_dict)
        results_dict['position_id'] = position_id
        transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
        if buy_dict['open_datetime'] > sell_dict['close_datetime']:
            print("Date Mismatch")
            print(buy_dict)
            print(sell_dict)
            print()
    except Exception as e:
        print(e)
        print("Error in transaction_dict")
        print(symbol)
        print(buy_dict)
        print(sell_dict)
        print(results_dict)
    return buy_dict, sell_dict, results_dict, transaction_dict, open_datetime

def buy_iterate_sellV2_invalerts(symbol, option_symbol, open_prices, strategy, polygon_df, position_id, trading_date, alert_hour):
    open_price = open_prices[0]
    # open_date = polygon_df['trading_date'][0]
    open_datetime = datetime(int(trading_date.split("-")[0]),int(trading_date.split("-")[1]),int(trading_date.split("-")[2]),int(alert_hour),0,0)
    contract_cost = round(open_price * 100,2)
    if strategy == "gainers" or strategy == "vdiff_gainC" or strategy == "ma":
        contract_type = "calls"
    elif strategy == "losers" or strategy == "vdiff_gainP" or strategy == "maP":
        contract_type = "puts"
        
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "quantity": 1, "contract_cost": contract_cost, "option_symbol": option_symbol, "position_id": position_id, "contract_type": contract_type}

    try:
        if strategy == "gainers":
            sell_dict = trade.time_decay_alpha_gainers_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        # elif strategy == "gainersP":
        #     sell_dict = trade.time_decay_alpha_gainersP_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "losers":
            sell_dict = trade.time_decay_alpha_losers_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        # elif strategy == "losersC":
        #     sell_dict = trade.time_decay_alpha_losersC_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "ma":
            sell_dict = trade.time_decay_alpha_ma_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "maP":
            sell_dict = trade.time_decay_alpha_maP_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "vdiff_gainC":
            sell_dict = trade.time_decay_alpha_vdiffC_v0_inv(polygon_df.iloc[1:],open_datetime,1)
        elif strategy == "vdiff_gainP":
            sell_dict = trade.time_decay_alpha_vdiffP_v0_inv(polygon_df.iloc[1:],open_datetime,1)
    except Exception as e:
        print(e)
        print("Error in sell_dict")
        print(symbol)
        return {}, {}, {}, {}, "error"
    
    # sell_dict['position_id'] = position_id
    try:
        sell_dict['position_id'] = position_id
        results_dict = backtrader_helper.create_results_dict(buy_dict, sell_dict)
        results_dict['position_id'] = position_id
        transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
        if buy_dict['open_datetime'] > sell_dict['close_datetime']:
            print("Date Mismatch")
            print(buy_dict)
            print(sell_dict)
            print()
    except Exception as e:
        print(e)
        print("Error in transaction_dict")
        print(symbol)
        print(buy_dict)
        print(sell_dict)
        print(results_dict)
    return buy_dict, sell_dict, results_dict, transaction_dict, open_datetime



def simulate_trades(data, datetimelist, starting_value, commission_cost, s3link):
    positions_list = []
    purchases_list = []
    sales_list =[]
    order_results_list = []
    order_id = 0

    for i, row in data.iterrows():
        transactions_list = []
        trades = []
        start_date, end_date, symbol, mkt_price, strategy, option_symbols, polygon_dfs = create_simulation_data(row)
        order_dt = start_date.strftime("%m+%d")
        pos_dt = start_date.strftime("%Y-%m-%d-%H")
        trade_data_pairs = []
        position_id = f"{row['symbol']}-{(row['title'].replace('_',''))}-{pos_dt}"
        contracts = []

        for df in polygon_dfs:
            contract_symbol = df.iloc[0]['ticker']
            price = df.iloc[0]['o']
            contracts.append({"contract_symbol": contract_symbol, "price": price})

        for df in polygon_dfs:
            open_prices = df['o'].values
            ticker = df.iloc[0]['ticker']
            buy_dict, sell_dict, results_dict, transaction_dict, open_datetime = buy_iterate_sellV2(symbol, ticker, open_prices, strategy, df, 1, position_id)
            if len(buy_dict) == 0 and len(sell_dict) == 0 and len(results_dict) == 0 and len(transaction_dict) == 0:
                print("Error in buy_iterate_sellV2")
                print(symbol)
                print(ticker)
                print(f"{order_id}_{order_dt}")
                continue

            buy_dict['order_id'] = f"{order_id}_{order_dt}"
            sell_dict['order_id'] = f"{order_id}_{order_dt}"

            transactions_list.append(transaction_dict)
            purchases_list.append(buy_dict)
            sales_list.append(sell_dict)
            order_results_list.append(results_dict)
            trades.append(results_dict)
            order_id += 1

        position_trades = {"position_id": position_id, "transactions": transactions_list, "open_datetime": start_date}
        positions_list.append(position_trades)
        
    return purchases_list, sales_list, order_results_list, positions_list

def simulate_trades_invalerts(data):
    positions_list = []
    purchases_list = []
    sales_list =[]
    order_results_list = []
    order_id = 0
    for i, row in data.iterrows():
        ## These variables are crucial for controlling the buy/sell flow of the simulation.
        alert_hour = row['hour']
        trading_date = row['date']
        trading_date = trading_date.split(" ")[0]
        year, month, day = trading_date.split("-")
        transactions_list = []
        trades = []
        start_date, end_date, symbol, mkt_price, strategy, option_symbols, enriched_options_aggregates = create_simulation_data_inv(row)
        # start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        order_dt = start_date.strftime("%m+%d")
        pos_dt = start_date.strftime("%Y-%m-%d-%H")
        trade_data_pairs = []
        position_id = f"{row['symbol']}-{(row['strategy'].replace('_',''))}-{pos_dt}"
        contracts = []

        # for df in enriched_options_aggregates:
        #     try:
        #         contract_symbol = df.iloc[0]['ticker']
        #         price = df.iloc[0]['o']
        #         avg_volume = df['v'].mean()
        #         if avg_volume < 40:
        #             print("low volume")
        #             continue
        #     except:
        #         print("error in contract_symbol")
        #         print(df)
        #         continue
        #     contracts.append({"contract_symbol": contract_symbol, "price": price})

        for df in enriched_options_aggregates:
            try:
                open_prices = df['o'].values
                ticker = df.iloc[0]['ticker']
                avg_volume = df['v'].mean()
                if avg_volume < 40:
                    print("low volume")
                    continue
                buy_dict, sell_dict, results_dict, transaction_dict, open_datetime = buy_iterate_sellV2_invalerts(symbol, ticker, open_prices, strategy, df, position_id, trading_date, alert_hour)
                if len(buy_dict) == 0 and len(sell_dict) == 0 and len(results_dict) == 0 and len(transaction_dict) == 0:
                    print("Error in buy_iterate_sellV2")
                    print(symbol)
                    print(ticker)
                    print(f"{order_id}_{order_dt}")
                    continue

                buy_dict['order_id'] = f"{order_id}_{order_dt}"
                sell_dict['order_id'] = f"{order_id}_{order_dt}"

                transactions_list.append(transaction_dict)
                purchases_list.append(buy_dict)
                sales_list.append(sell_dict)
                order_results_list.append(results_dict)
                trades.append(results_dict)
                order_id += 1
            except Exception as e:
                print("error in buy_iterate_sellV2_invalerts")
                print(df)
                continue

        position_trades = {"position_id": position_id, "transactions": transactions_list, "open_datetime": datetime(int(year),int(month),int(day),int(alert_hour),0,0)}
        positions_list.append(position_trades)
    
    print("trades done")
    return purchases_list, sales_list, order_results_list, positions_list

        


if __name__ == "__main__":
    s3link = {
    'bucketname': 'icarus-research-data',
    'objectkey': 'training_datasets/expanded_1d_datasets/2023/05/12.csv'
    }