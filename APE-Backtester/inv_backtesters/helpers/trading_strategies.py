from datetime import datetime, timedelta
import logging
from helpers.helper import get_day_diff
import numpy as np  
import math
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)

### INV ALERTS STRATEGIES ###

# def time_decay_alpha_gainers_v0_inv(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     option_open = polygon_df.iloc[0]['o']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = .027
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .014)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > (2*Target_pct):
#             Floor_pct += 0.012
#         elif pct_change > Target_pct:
#             Floor_pct += 0.0095

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change < (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": polygon_df.iloc[-1]['o']*100, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_ma_v0_inv(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = .0225
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .012)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > (2*Target_pct):
#             Floor_pct += 0.01
#         elif pct_change > Target_pct:
#             Floor_pct += 0.0075

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change < (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_maP_v0_inv(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = -.023
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .012)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change < (2*Target_pct):
#             Floor_pct -= 0.01
#         elif pct_change < Target_pct:
#             Floor_pct -= 0.075

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict


def time_decay_alpha_BFP_v0_inv(polygon_df, simulation_date, quantity,config):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.025
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) + (.012 + (-1*config['risk_adjustment']))

        if pct_change < (2*Target_pct):
            Floor_pct -= 0.01
        elif pct_change < Target_pct:
            Floor_pct -= 0.075

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if pct_change > Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC_v0_inv(polygon_df, simulation_date, quantity,config):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .025
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (.012 + (-1*config['risk_adjustment']))

        if pct_change > (2*Target_pct):
            Floor_pct += 0.01
        elif pct_change > Target_pct:
            Floor_pct += 0.0075


        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if pct_change < Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        

def time_decay_alpha_BFP1D_v0_inv(polygon_df, simulation_date, quantity,config):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.015
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) + (.007 + (-1*config['risk_adjustment']))


        if pct_change < (2*Target_pct):
            Floor_pct -= 0.006
        elif pct_change < Target_pct:
            Floor_pct -= 0.035

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 1:
            if pct_change > Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        
def time_decay_alpha_BFC1D_v0_inv(polygon_df, simulation_date, quantity,config):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .015
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (.007 + (-1*config['risk_adjustment']))

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > (2*Target_pct):
            Floor_pct += 0.006
        elif pct_change > Target_pct:
            Floor_pct += 0.0035

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 1:
            if pct_change < Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        

# def time_decay_alpha_losers_v0_inv(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = -.026
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .013)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change < (2*Target_pct):
#             Floor_pct -= 0.01
#         elif pct_change < Target_pct:
#             Floor_pct -= 0.0075

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict


# def time_decay_alpha_vdiffC_v0_inv(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = .0225
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .012)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > (2*Target_pct):
#             Floor_pct += 0.01
#         elif pct_change > Target_pct:
#             Floor_pct += 0.0075

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change < (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_vdiffP_v0_inv(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = -.025
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .012)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change < (2*Target_pct):
#             Floor_pct -= 0.01
#         elif pct_change < Target_pct:
#             Floor_pct -= 0.0075

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict


# ### 1DAY TRADING ALGORITHMS

# def time_decay_alpha_gainers_v0_inv1D(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     option_open = polygon_df.iloc[0]['o']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = .017
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .01)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > (2*Target_pct):
#             Floor_pct += 0.005
#         elif pct_change > Target_pct:
#             Floor_pct += 0.0025

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change < (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": polygon_df.iloc[-1]['o']*100, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_ma_v0_inv1D(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = .014
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .007)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > (2*Target_pct):
#             Floor_pct += 0.006
#         elif pct_change > Target_pct:
#             Floor_pct += 0.0045

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change < (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_maP_v0_inv1D(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = -.015
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .009)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change < (2*Target_pct):
#             Floor_pct -= 0.004
#         elif pct_change < Target_pct:
#             Floor_pct -= 0.002

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_losers_v0_inv1D(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = -.0175
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .01)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change < (2*Target_pct):
#             Floor_pct -= 0.006
#         elif pct_change < Target_pct:
#             Floor_pct -= 0.003

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict


# def time_decay_alpha_vdiffC_v0_inv1D(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = .0155
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .009)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > (2*Target_pct):
#             Floor_pct += 0.004
#         elif pct_change > Target_pct:
#             Floor_pct += 0.002

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change < (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_vdiffP_v0_inv1D(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         Target_pct = -.016
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .01)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change < (2*Target_pct):
#             Floor_pct -= 0.004
#         elif pct_change < Target_pct:
#             Floor_pct -= 0.002

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100), "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict


# ### RELATIVE VOL FUNCTIONS ###
# def time_decay_alpha_vdiffP_v0_rvol(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         strategy_vol_expectation = 1.2
#         Target_pct = (row['threeD_stddev50'] * strategy_vol_expectation)
#         floor_modifier = (Target_pct * 0.8)
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + floor_modifier)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > Target_pct:
#             Floor_pct -= 0.005

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code == 2:
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict

# def time_decay_alpha_vdiffP_v0_rvol(polygon_df, simulation_date, quantity):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     for index, row in polygon_df.iterrows():
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         strategy_vol_expectation = 1.2
#         Target_pct = (row['threeD_stddev50'] * strategy_vol_expectation)
#         floor_modifier = (Target_pct * 0.8)
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + floor_modifier)

#         # if type(Floor_pct) == float:
#         #     Floor_pct = -0.02
#         if pct_change > Target_pct:
#             Floor_pct -= 0.005

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff = get_business_days(simulation_date, row['date'])
#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#         elif day_diff > 3:
#             sell_code = 2
#             reason = "Held through confidence."
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 2
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= Target_pct:
#                 sell_code = 2
#                 reason = "Hit exit target, sell."
#             elif pct_change > (.5*(Target_pct)):
#                 sell_code = 2
#                 reason = "Failed momentum gate, sell."
#             else:
#                 sell_code = 0
#                 reason = "Hold."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index)
#             sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100), "option_symbol": row['ticker'],"reason": reason}
#             return sell_dict
        
#     sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
#     return sell_dict


### VALUE CAPTURE FUNCTIONS ###

def time_decay_alpha_BFP_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.025
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(row['o']) - float(derivative_open_price))/float(derivative_open_price))
        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price) + (.012 + (-1*config['risk_adjustment'])))


        if pct_change < (2*Target_pct):
            Floor_pct -= 0.01
        elif pct_change < Target_pct:
            Floor_pct -= 0.075

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""

        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            Floor_pct -= float(risk_pct)
        elif day_diff < 2:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if pct_change > Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        


def time_decay_alpha_BFC_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .025
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(row['o']) - float(derivative_open_price))/float(derivative_open_price))
        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price) - (.012 + (-1*config['risk_adjustment'])))



        if pct_change > (2*Target_pct):
            Floor_pct += 0.01
        elif pct_change > Target_pct:
            Floor_pct += 0.0075

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            Floor_pct += float(risk_pct)
        elif day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if pct_change < Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict
        


def time_decay_alpha_BFP1D_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.015
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(row['o']) - float(derivative_open_price))/float(derivative_open_price))
        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price) + (.007 + (-1*config['risk_adjustment'])))


        if pct_change < (2*Target_pct):
            Floor_pct -= 0.01
        elif pct_change < Target_pct:
            Floor_pct -= 0.075

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""

        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            Floor_pct -= float(risk_pct)
        elif day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 1:
            if pct_change > Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict


def time_decay_alpha_BFC1D_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .015
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(row['o']) - float(derivative_open_price))/float(derivative_open_price))
        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price) - (.012 + (-1*config['risk_adjustment'])))

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > (2*Target_pct):
            Floor_pct += 0.01
        elif pct_change > Target_pct:
            Floor_pct += 0.0075

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            Floor_pct += float(risk_pct)
        elif day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 1:
            if pct_change < Floor_pct:
                sell_code = 2
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 2
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
            return sell_dict


### BET SIZING FUNCTIONS ###

def build_trade(position, risk_unit,put_adjustment,portfolio_cash):
    buy_orders = []
    sell_orders = []
    contract_costs = []
    transactions = position['transactions']
    contract_type = ""
    for transaction in transactions:
        # print(type(trade_info))
        # print(position_id)
        # print(trade_info[0])
        transaction['sell_info']['close_trade_dt'] = transaction['close_trade_dt']
        buy_orders.append(transaction['buy_info'])
        sell_orders.append(transaction['sell_info'])
        contract_costs.append(transaction['buy_info']['contract_cost'])
        contract_type = transaction['buy_info']['contract_type']
    
    sized_buys, sized_sells = bet_sizer(contract_costs, buy_orders, sell_orders, risk_unit, contract_type,put_adjustment,portfolio_cash)
    if sized_buys == None:
        print("ERROR in build_trade, no trades")
        print(position)
    return sized_buys, sized_sells

def bet_sizer(contract_costs,buy_orders,sell_orders,risk_unit,contract_type,put_adjustment,portfolio_cash):
    ## FUNDS ADJUSTMENT
    available_funds = portfolio_cash
    ## PUT ADJUSTMENT
    if contract_type == "calls":
        target_cost = (risk_unit * available_funds)
    elif contract_type == "puts":
        target_cost = ((risk_unit * available_funds)*put_adjustment)
    else:
        target_cost = (risk_unit * available_funds)
        print("ERROR")
        print(buy_orders)
        print(contract_type)
    spread_cost = sum(contract_costs)
    quantities = finalize_trade(buy_orders, spread_cost, target_cost)

    if len(quantities) == 0:
        return None, None
    for i, value in enumerate(quantities):
        try:
            if value == 0:
                buy_orders[i] = None
                sell_orders[i] = None
            else:
                try:
                    buy_orders[i]['quantity'] = value
                    sell_orders[i]['quantity'] = value
                except Exception as e:
                    print(f"Error {e} in size_trade {i} {value}")
                    print(e)
                    print(buy_orders)
                    return [], []
        except Exception as e:
            print("size_trade 2")
            print(e)
            print(buy_orders)
            return [], []

    return buy_orders, sell_orders
    

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
        

def build_trade_analytics(row, polygon_df, derivative_open_price, index, quantity, reason):
    trade_dict = {}
    before_df = polygon_df.iloc[:index]
    after_df = polygon_df.iloc[index:]
    trade_dict['max_value_before'] = before_df['h'].max()
    trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
    trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date']
    trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)

    if len(after_df) > 0:
        trade_dict['max_value_after'] = after_df['h'].max()
        trade_dict['max_value_after_idx'] = after_df['h'].idxmax()
        trade_dict['max_value_after_date'] = after_df.loc[trade_dict['max_value_after_idx']]['date']
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
    trade_dict["reason"] = reason
    return trade_dict
