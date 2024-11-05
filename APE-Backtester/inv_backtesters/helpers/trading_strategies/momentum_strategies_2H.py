from datetime import datetime, timedelta
import logging
from helpers.helper import get_day_diff, get_hour_diff
# from helpers.backtest_functions import build_trade_analytics
import numpy as np  
import math
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def tda_PUT2H_CDVOLVARVC(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num,symbol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    Floor_pct = (-vol * config['volatility_threshold'])
    if order_num > 4:
        order_num = 4
    vc_values = config['vc_level'].split('+')
    vc_config = build_vc_config(vc_values,simulation_date,symbol)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        hour = row['date'].hour
        minute = row['date'].minute

        if deriv_pct_change > int(vc_config[order_num]):
            Floor_pct = (.8*underlying_gain)
            if pct_change <= Floor_pct:
                reason = f"VCSell{order_num}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
                return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        hour_diff = get_hour_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if hour == 15 and minute >= 50:
            sell_code = 7
            reason = "End of day, sell."
        elif hour_diff < 1:
            if pct_change > Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct"
        elif hour_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
            return sell_dict
        elif hour_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= target_pct:
                Floor_pct = (.8*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold",order_num)
    return sell_dict

def tda_CALL2H_CDVOLVARVC(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num,symbol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    Floor_pct = (-vol * config['volatility_threshold'])
    if order_num > 4:
        order_num = 4
    vc_values = config['vc_level'].split('+')
    vc_config = build_vc_config(vc_values,simulation_date,symbol)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        hour = row['date'].hour
        minute = row['date'].minute

        if deriv_pct_change > int(vc_config[order_num]):
            Floor_pct = (.8*underlying_gain)
            if pct_change <= Floor_pct:
                reason = f"VCSell{order_num}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
                return sell_dict


        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        hour_diff = get_hour_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if hour == 15 and minute >= 50:
            sell_code = 7
            reason = "End of day, sell."
        elif hour_diff < 1:
            if pct_change < Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct"
        elif hour_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
            return sell_dict
        elif hour_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= target_pct:
                Floor_pct = (.8*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)
            return sell_dict
        
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold",order_num)
    return sell_dict

# def tda_PUT2H_CDVOLSTEP(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num,symbol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     Floor_pct = (-vol * config['volatility_threshold'])
#     if order_num > 4:
#         order_num = 4
#     vc_values = config['vc_level'].split('+')
#     vc_config = build_vc_config(vc_values,simulation_date,symbol)
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         min_value = polygon_df.iloc[:index]['underlying_price'].min()
#         underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         hour = row['date'].hour
#         minute = row['date'].minute

#         if deriv_pct_change > int(vc_config[order_num]):
#             Floor_pct = (.8*underlying_gain)
#             if pct_change <= Floor_pct:
#                 reason = f"VCSell{order_num}"
#                 sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
#                 return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         hour_diff = get_hour_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if hour == 15 and minute >= 50:
#             sell_code = 7
#             reason = "End of day, sell."
#         elif hour_diff < 1:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct"
#         elif hour_diff > 2:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
#             return sell_dict
#         elif hour_diff == 1:
#             Floor_pct = Floor_pct * config['vc_step']
#             if pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= target_pct:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold",order_num)
#     return sell_dict

# def tda_CALL2H_CDVOLSTEP(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num,symbol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     Floor_pct = (-vol * config['volatility_threshold'])
#     vc_values = config['vc_level'].split('+')
#     vc_config = build_vc_config(vc_values,simulation_date,symbol)
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         hour = row['date'].hour
#         minute = row['date'].minute

#         if deriv_pct_change > int(vc_config[order_num]):
#             Floor_pct = (.8*underlying_gain)
#             if pct_change <= Floor_pct:
#                 reason = f"VCSell{order_num}"
#                 sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
#                 return sell_dict


#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         hour_diff = get_hour_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if hour == 15 and minute >= 50:
#             sell_code = 7
#             reason = "End of day, sell."
#         elif hour_diff < 1:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct"
#         elif hour_diff > 2:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
#             return sell_dict
#         elif hour_diff == 1:
#             Floor_pct = Floor_pct * config['vc_step']
#             if pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= target_pct:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)
#             return sell_dict
        
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold",order_num)
#     return sell_dict

def tda_PUT2H_CDVOLSTEP(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num,symbol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    Floor_pct = (-vol * config['volatility_threshold'])
    vc_values = config['vc_level'].split('+')
    vc_config = build_vc_config(vc_values,simulation_date,symbol)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        hour = row['date'].hour
        minute = row['date'].minute
        hour_diff = get_hour_diff(simulation_date, row['date'])

        if deriv_pct_change > int(vc_config[order_num]):
            if order_num > 3:
                Floor_pct = (5.*underlying_gain)
            elif order_num <= 3:
                Floor_pct = (9.*underlying_gain)

            if pct_change <= Floor_pct:
                reason = f"VCSell{order_num}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
                return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")

        sell_code = 0
        reason = ""
        if hour == 15 and minute >= 50:
            sell_code = 7
            reason = "End of day, sell."
        elif hour_diff < 1:
            if pct_change > Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct"
        elif hour_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
            return sell_dict
        elif hour_diff == 1:
            step = float(config['vc_step'].split('+')[0])
            Floor_pct = Floor_pct * step
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= target_pct:
                if hour_diff < 1.5:
                    Floor_pct = (.8*underlying_gain)
                else:
                    Floor_pct = (.95*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold",order_num)
    return sell_dict

def tda_CALL2H_CDVOLSTEP(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num,symbol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    Floor_pct = (-vol * config['volatility_threshold'])
    vc_values = config['vc_level'].split('+')
    vc_config = build_vc_config(vc_values,simulation_date,symbol)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        hour = row['date'].hour
        minute = row['date'].minute

        if deriv_pct_change > int(vc_config[order_num]):
            if order_num > 3:
                Floor_pct = (5.*underlying_gain)
            elif order_num <= 3:
                Floor_pct = (9.*underlying_gain)

            if pct_change <= Floor_pct:
                reason = f"VCSell{order_num}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
                return sell_dict


        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        hour_diff = get_hour_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if hour == 15 and minute >= 50:
            sell_code = 7
            reason = "End of day, sell."
        elif hour_diff < 1:
            if pct_change < Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct"
        elif hour_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)  
            return sell_dict
        elif hour_diff == 1:
            step = float(config['vc_step'].split('+')[0])
            Floor_pct = Floor_pct * step
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= target_pct:
                if hour_diff < 1.5:
                    Floor_pct = (.8*underlying_gain)
                else:
                    Floor_pct = (.95*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason,order_num)
            return sell_dict
        
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,"never sold",order_num)
    return sell_dict



def build_vc_config(vc_values,simulation_date,symbol):
    day_of_week = simulation_date.weekday()
    if symbol in ["SPY","QQQ","IWM"]:
        vc_config = {
            0:  int(vc_values[0]),
            1: int(vc_values[1]),
            2: int(vc_values[2]),
            3: int(vc_values[3]),
            4: int(vc_values[4]),
            5: int(vc_values[4])
        }
    elif day_of_week == 0:
        vc_config = {
        0:  int(vc_values[0])-15,
        1: int(vc_values[1])-15,
        2: int(vc_values[2])-15,
        3: int(vc_values[3])-15,
        4: int(vc_values[4])-15,
        5: int(vc_values[4])-15
    }
    elif day_of_week == 1:
        vc_config = {
        0:  int(vc_values[0])-10,
        1: int(vc_values[1])-10,
        2: int(vc_values[2])-10,
        3: int(vc_values[3])-10,
        4: int(vc_values[4])-10,
        5: int(vc_values[4])-10
    }
    elif day_of_week == 2:
        vc_config = {
        0:  int(vc_values[0])-5,
        1: int(vc_values[1])-5,
        2: int(vc_values[2])-5,
        3: int(vc_values[3])-5,
        4: int(vc_values[4])-5,
        5: int(vc_values[4])-5
    }
    elif day_of_week in [3,4]:
        vc_config = {
        0:  int(vc_values[0]),
        1: int(vc_values[1]),
        2: int(vc_values[2]),
        3: int(vc_values[3]),
        4: int(vc_values[4]),
        5: int(vc_values[4])
    }

    # vc_config = {
    #     0:  int(vc_values[0]),
    #     1: int(vc_values[1]),
    #     2: int(vc_values[2]),
    #     3: int(vc_values[3]),
    #     4: int(vc_values[3])
    # }
    return vc_config

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