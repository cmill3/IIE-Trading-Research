from datetime import timedelta, datetime
import datetime as dt
import pandas as pd
from helpers import backtrader_helper
import warnings
from helpers.trading_strategies import time_decay_alpha_ma_v0, time_decay_alpha_maP_v0, time_decay_alpha_gainers_v0, time_decay_alpha_losers_v0, bet_sizer
# from pandas._libs.mode_warnings import SettingWithCopyWarning


# warnings.filterwarnings("ignore", category=FutureWarning)
# warnings.filterwarnings("ignore", category=DeprecationWarning)
# warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


def kickoff(s3link):
    starting_value = backtrader_helper.startbacktrader(1000000)
    commission_cost = 0.35
    raw_data = backtrader_helper.s3_data(s3link)
    dataset = pd.DataFrame(raw_data)
    full_data = backtrader_helper.pull_modeling_results(dataset)
    data = full_data[full_data.volume >= 20000000]
    data = data.loc[data['classifier_prediction'] > .5]
    data.reset_index(inplace=True)
    init_date = datetime.strptime(raw_data['date'].values[0], '%Y-%m-%d %H:%M:%S')
    comp_date = backtrader_helper.create_end_date(init_date, 4)
    datetime_list, datetime_index, results = backtrader_helper.build_table(init_date, comp_date)
    return starting_value, commission_cost, raw_data, data, datetime_list, results

def create_simulation_data(row):
    # start_date = datetime.strptime(row['date'], '%Y-%m-%d %H:%M:%S')
    end_date = backtrader_helper.create_end_date(row['date'], 3)
    symbol = row['symbol']
    mkt_price = row['regularMarketPrice']
    contracts = row['contracts']
    strategy = row['title']
    # option_symbol, polygon_dfs = backtrader_helper.data_pull(symbol, start_date, end_date, mkt_price, strategy, contracts)
    trading_aggregates, option_symbols = backtrader_helper.create_options_aggs(symbol, row['date'], end_date, mkt_price, strategy, contracts, spread_length=3)
    return row['date'], end_date, symbol, mkt_price, strategy, option_symbols, trading_aggregates

def buy_iterate_sellV2(symbol, mkt_price, option_symbol, open_prices, strategy, polygon_df, commission_cost, quantity):
    open_price = open_prices[0]
    open_dt = polygon_df['date'][0]
    open_datetime = open_dt.to_pydatetime()
    # open_dt_hrmin = open_datetime.strftime("%Y-%m-%d %H:%M")
    # transaction_cost = commission_cost * contract_number
    # total_transaction_cost = transaction_cost * 2
    contract_cost = round((open_price * 100 * quantity), 2)
    buy_dict = {"open_price": open_price, "open_datetime": open_datetime, "quantity": quantity, "contract_cost": contract_cost, "option_symbol": option_symbol}

    try:
        if strategy == "day_gainers":
            sell_dict = time_decay_alpha_gainers_v0(polygon_df.iloc[1:],open_datetime,quantity)
        elif strategy == "day_losers":
            sell_dict = time_decay_alpha_losers_v0(polygon_df.iloc[1:],open_datetime,quantity)
        elif strategy == "maP":
            sell_dict = time_decay_alpha_maP_v0(polygon_df.iloc[1:],open_datetime,quantity)
        elif strategy == "most_actives":
            sell_dict = time_decay_alpha_ma_v0(polygon_df.iloc[1:],open_datetime,quantity)
    except Exception as e:
        print(e)
        print("Error in sell_dict")
        print(symbol)
        return {}, {}, {}, {}
    
    try:
        results_dict = backtrader_helper.create_results_dict(buy_dict, sell_dict)
        transaction_dict = {"buy_dict": buy_dict, "sell_dict":sell_dict, "results_dict": results_dict}
    except Exception as e:
        print(e)
        print("Error in transaction_dict")
        print(symbol)
        print(buy_dict)
        print(sell_dict)
        print(results_dict)
    return buy_dict, sell_dict, results_dict, transaction_dict



def simulate_trades(data, datetimelist, starting_value, commission_cost, s3link):
    transactions_list = []
    positions_list = []
    purchases_list = []
    sales_list =[]
    order_results_list = []
    for i, row in data.iterrows():
        trades = []
        start_date, end_date, symbol, mkt_price, strategy, option_symbols, polygon_dfs = create_simulation_data(row)
        pos_dt = start_date.strftime("%Y-%m-%d-%H")
        trade_data_pairs = []
        position_id = f"{row['symbol']}-{(row['title'].replace('_',''))}-{pos_dt}"
        contracts = []
        for df in polygon_dfs:
            contract_symbol = df.iloc[0]['ticker']
            price = df.iloc[0]['o']
            contracts.append({"contract_symbol": contract_symbol, "price": price})
        contracts, quantity = bet_sizer(contracts,start_date)
        for df in polygon_dfs:
            open_prices = df['o'].values
            ticker = df.iloc[0]['ticker']
            buy_dict, sell_dict, results_dict, transaction_dict = buy_iterate_sellV2(symbol, mkt_price, ticker, open_prices, strategy, df, commission_cost, quantity)
            if len(buy_dict) == 0 and len(sell_dict) == 0 and len(results_dict) == 0 and len(transaction_dict) == 0:
                print("Error in buy_iterate_sellV2")
                print(symbol)
                print(ticker)
                continue
            transactions_list.append(transaction_dict)
            purchases_list.append(buy_dict)
            sales_list.append(sell_dict)
            order_results_list.append(results_dict)
            trades.append(results_dict)
        position_trades = {position_id: trades}
        positions_list.append(position_trades)
        
    return purchases_list, sales_list, order_results_list, positions_list
        


if __name__ == "__main__":
    s3link = {
    'bucketname': 'icarus-research-data',
    'objectkey': 'training_datasets/expanded_1d_datasets/2023/05/12.csv'
    }
    starting_value, commission_cost, rawdata, data, datetime_list, datetimeindex, results,  = kickoff(s3link)
    transactions = btfunction(data, datetime_list, starting_value, commission_cost, s3link)
