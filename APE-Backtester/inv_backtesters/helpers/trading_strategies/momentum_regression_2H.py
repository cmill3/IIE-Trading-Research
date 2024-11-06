from datetime import datetime, timedelta
import logging
from helpers.helper import get_day_diff, get_hour_diff
# from helpers.backtest_functions import build_trade_analytics
import numpy as np  
import math
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def tda_PUT2H_REG(polygon_df, simulation_date, quantity, config, target, vol,symbol):
    volatility = polygon_df['underlying_vol'].iloc[0]
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']

    for index, row in polygon_df.iterrows():
        if index == 0:
            continue

        hour = row['date'].hour
        minute = row['date'].minute
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        hour_diff = get_hour_diff(simulation_date, row['date'])

        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_pct_change = ((float(max_value) - float(open_price))/float(open_price))
        current_pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        print(f"Target: {target}")
        print(f"Max pct change: {max_pct_change}")
        print(f"Current pct change: {current_pct_change}")
        downside_tolerance = calculate_max_gain_and_tolerance(target, max_pct_change, config,"put")

        sell_code = 0
        reason = ""
        if hour == 15 and minute >= 50:
            sell_code = 7
            reason = "End of day, sell."
        elif hour_diff < 1:
            if current_pct_change > downside_tolerance:
                sell_code = 2
                reason = f"Breached downside tolerance"
        elif hour_diff >= 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,"reg")  
            return sell_dict
        elif hour_diff == 1:
            step_1, step_2 = config['vol_step'].split('+')
            step_1 = float(step_1)
            step_2 = float(step_2)
            if hour_diff < 1.5:
                adjusted_tolerance = downside_tolerance * step_1
                if current_pct_change > adjusted_tolerance:
                    sell_code = 4
                    reason = "vol step1"
            else:
                adjusted_tolerance = downside_tolerance * step_2
                if current_pct_change > adjusted_tolerance:
                    sell_code = 4
                    reason = "vol step2"

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,"reg")
            return sell_dict
        
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold","reg")
    return sell_dict

def tda_CALL2H_REG(polygon_df, simulation_date, quantity, config, target, vol,symbol):
    volatility = polygon_df['underlying_vol'].iloc[0]
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']

    for index, row in polygon_df.iterrows():
        if index == 0:
            continue

        hour = row['date'].hour
        minute = row['date'].minute
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        hour_diff = get_hour_diff(simulation_date, row['date'])

        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_pct_change = ((float(max_value) - float(open_price))/float(open_price))
        current_pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        downside_tolerance = calculate_max_gain_and_tolerance(target, max_pct_change, config,"call")


        sell_code = 0
        reason = ""
        if hour == 15 and minute >= 50:
            sell_code = 7
            reason = "End of day, sell."
        elif hour_diff < 1:
            if current_pct_change < downside_tolerance:
                sell_code = 2
                reason = f"Breached downside tolerance"
        elif hour_diff >= 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,"reg")  
            return sell_dict
        elif hour_diff == 1:
            step_1, step_2 = config['vol_step'].split('+')
            step_1 = float(step_1)
            step_2 = float(step_2)
            if hour_diff < 1.5:
                adjusted_tolerance = downside_tolerance * step_1
                if current_pct_change < adjusted_tolerance:
                    sell_code = 4
                    reason = "vol step1"
            else:
                adjusted_tolerance = downside_tolerance * step_2
                if current_pct_change < adjusted_tolerance:
                    sell_code = 4
                    reason = "vol step2"

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,"reg")
            return sell_dict
        
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold","reg")
    return sell_dict

def calculate_max_gain_and_tolerance(target_pct_change, max_pct_change, config, call_put):
    if call_put == "call":
        starting_tolerance = -(target_pct_change)
        if max_pct_change <= 0:
            downside_tolerance = starting_tolerance
        else:
            downside_tolerance = starting_tolerance + max_pct_change
        adjusted_tolerance = downside_tolerance * config['volatility_threshold']
        return adjusted_tolerance
    elif call_put == "put":
        starting_tolerance = -(target_pct_change)
        print(f"Starting tolerance: {starting_tolerance}")
        if max_pct_change >= 0:
            downside_tolerance = starting_tolerance
        else:
            downside_tolerance = starting_tolerance + max_pct_change
        adjusted_tolerance = downside_tolerance * config['volatility_threshold']
        print(f"Adjusted tolerance: {adjusted_tolerance}")
        print()
        return adjusted_tolerance

def build_trade_analytics(row, polygon_df, derivative_open_price, index, quantity, sell_code,order_num):
    trade_dict = {}
    before_df = polygon_df.iloc[:index]
    after_df = polygon_df.iloc[(index+1):]
    trade_dict['max_value_before'] = before_df['h'].max()
    trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
    trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date'].strftime("%Y-%m-%d %H:%M")
    trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)
    trade_dict['order_num'] = order_num

    if len(after_df) > 0:
        trade_dict['max_value_after'] = after_df['h'].max()
        trade_dict['max_value_after_idx'] = after_df['h'].idxmax()
        trade_dict['max_value_after_date'] = after_df.loc[trade_dict['max_value_after_idx']]['date'].strftime("%Y-%m-%d %H:%M")
        trade_dict['max_value_after_pct_change'] = ((trade_dict['max_value_after'] - derivative_open_price)/derivative_open_price)
    else:
        trade_dict['max_value_after'] = None
        trade_dict['max_value_after_idx'] = None
        trade_dict['max_value_after_date'] = None
        trade_dict['max_value_after_pct_change'] = None

    trade_dict["close_price"] = row['o']
    trade_dict["close_datetime"] = row['date'].to_pydatetime()
    trade_dict["quantity"] = quantity
    trade_dict["contract_cost"] = (row['o']*100)
    trade_dict["option_symbol"] = row['ticker']
    trade_dict["sell_code"] = sell_code
    return trade_dict