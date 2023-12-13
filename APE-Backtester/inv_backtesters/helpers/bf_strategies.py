from datetime import datetime, timedelta
import logging
from helpers.helper import get_day_diff
from helpers.strategy_helper import build_trade_analytics
import numpy as np  
import math
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def time_decay_alpha_BFP_v0_cls(polygon_df, simulation_date, quantity,config):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        Target_pct = -.025
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (.012 + (-1*config['risk_adjustment']))

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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC_v0_cls(polygon_df, simulation_date, quantity,config):
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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        

def time_decay_alpha_BFP1D_v0_cls(polygon_df, simulation_date, quantity,config):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        Target_pct = -.015
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (.007 + (-1*config['risk_adjustment']))


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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        
def time_decay_alpha_BFC1D_v0_cls(polygon_df, simulation_date, quantity,config):
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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


### VALUE CAPTURE FUNCTIONS ###

def time_decay_alpha_BFP_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = -.025
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price) + (.012 + (-1*config['risk_adjustment'])))


        if pct_change < (2*Target_pct):
            Floor_pct -= 0.01
        elif pct_change < Target_pct:
            Floor_pct -= 0.075

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""

        vc_amt,risk_pct = config['vc_level'].split("$")

        if max_deriv_value > float(vc_amt):
            Floor_pct -= float(risk_pct)
        
        if day_diff < 3:
            if deriv_pct_change > float(vc_amt):
                Floor_pct = Floor_pct
            else:
                Floor_pct = -1 * Target_pct
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell early."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        


def time_decay_alpha_BFC_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = .025
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
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

        if max_deriv_value > float(vc_amt):
            Floor_pct += float(risk_pct)
        
        if day_diff < 3:
            if deriv_pct_change > float(vc_amt):
                Floor_pct = Floor_pct
            else:
                Floor_pct = -1 * Target_pct
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell early."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        


def time_decay_alpha_BFP1D_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = -.015
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price) + (.007 + (-1*config['risk_adjustment'])))


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
        
        if day_diff < 1:
            if deriv_pct_change > float(vc_amt):
                Floor_pct = Floor_pct
            else:
                Floor_pct = -1 * Target_pct
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell early."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict


def time_decay_alpha_BFC1D_v0_vc(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = .015
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
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
        
        if day_diff < 1:
            if deriv_pct_change > float(vc_amt):
                Floor_pct = Floor_pct
            else:
                Floor_pct = -1 * Target_pct
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell early."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        

### VC SELL MODELS

def time_decay_alpha_BFP_v0_vcSell(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = -.025
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100 
        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price) + (.012 + (-1*config['risk_adjustment'])))


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
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
        if day_diff < 2:
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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        


def time_decay_alpha_BFC_v0_vcSell(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = .025
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
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
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
        if day_diff < 2:
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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        


def time_decay_alpha_BFP1D_v0_vcSell(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = -.015
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price) + (.007 + (-1*config['risk_adjustment'])))


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
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
        if day_diff < 1:
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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict

def time_decay_alpha_BFC1D_v0_vcSell(polygon_df, simulation_date, quantity,config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        Target_pct = .015
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
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
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
        if day_diff < 1:
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
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= Target_pct:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(Target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        

### REGRESSION MODELS ###
def time_decay_alpha_BFP_v0_regVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        elif min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFC_v0_regVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100 

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")


        if deriv_pct_change > float(vc_amt):
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        elif max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)


        print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 2:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFP1D_v0_regVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        elif min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC1D_v0_regVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")


        if deriv_pct_change > float(vc_amt):
            sell_code = 1
            reason = "Derivative Value Capture."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        elif max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)


        print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."
            else:
                sell_code = 0
                reason = "Hold."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        

def time_decay_alpha_BFP_v0_regVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            risk -= float(risk_adj2)
        elif min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFC_v0_regVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")        
        vc_amt,risk_pct = config['vc_level'].split("$")


        if deriv_pct_change > float(vc_amt):
            risk -= float(risk_adj2)
        elif max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)


        print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 2:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFP1D_v0_regVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")

        if deriv_pct_change > float(vc_amt):
            risk -= float(risk_adj2)
        elif min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC1D_v0_regVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")


        if deriv_pct_change > float(vc_amt):
            risk -= float(risk_adj2)
        elif max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)


        print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        
def time_decay_alpha_BFP_v0_reg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")        
        # vc_amt,risk_pct = config['vc_level'].split("$")

        if min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)


        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
    
        


def time_decay_alpha_BFC_v0_reg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")


        if max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 2:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFP1D_v0_reg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")

        if min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC1D_v0_reg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")


        if max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)


        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict


### AGGRESSIVE HOLD ###

def time_decay_alpha_BFP_v0_regAgg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_hit = False
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        floor_value = (config['floor_value'] * min_pct_change)
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")        
        # vc_amt,risk_pct = config['vc_level'].split("$")

        if min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)


        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            # if target_hit and pct_change >= floor_value:
            #     sell_code = 2
            #     reason = "Hit exit target, sell."
            if pct_change <= regression_tgt:
                # target_hit = True
                sell_code = 2
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif target_hit and pct_change >= floor_value:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change <= regression_tgt:
                target_hit = True
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
    
        


def time_decay_alpha_BFC_v0_regAgg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_hit = False
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        floor_value = (config['floor_value'] * max_pct_change)
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")


        if max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            # if target_hit and pct_change <= floor_value:
            #     sell_code = 2
            #     reason = "Hit exit target, sell."
            # el
            if pct_change >= regression_tgt:
                # target_hit = True
                sell_code = 2
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 2:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif target_hit and pct_change <= floor_value:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change >= regression_tgt:
                target_hit = True
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFP1D_v0_regAgg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_hit = False
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
        floor_value = (config['floor_value'] * min_pct_change)

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")

        if min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            # if target_hit and pct_change >= floor_value:
            #     sell_code = 2
            #     reason = "Hit exit target, sell."
            # el
            if pct_change <= regression_tgt:
                sell_code = 2
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif target_hit and pct_change >= floor_value:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change <= regression_tgt:
                target_hit = True
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC1D_v0_regAgg(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_hit = False
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100
        floor_value = (config['floor_value'] * max_pct_change)

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")


        if max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)


        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            # if target_hit and pct_change <= floor_value:
            #     sell_code = 2
            #     reason = "Hit exit target, sell."
            # el
            if pct_change >= regression_tgt:
                sell_code = 2
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif target_hit and pct_change <= floor_value:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change >= regression_tgt:
                target_hit = True
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict


def time_decay_alpha_BFP_v0_regAggVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")        
        vc_amt,risk_pct = config['vc_level'].split("$")

        if min_pct_change < -(2*vol):
            risk -= float(risk_adj2)
        elif min_pct_change < -vol:
            risk -= float(risk_adj1)


        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if deriv_pct_change > float(vc_amt):
                sell_code = 1
                reason = "Derivative Value Capture, sell."
            elif pct_change <= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
    
        


def time_decay_alpha_BFC_v0_regAggVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")

        if max_pct_change > (2*vol):
            risk -= float(risk_adj2)
        elif max_pct_change > vol:
            risk -= float(risk_adj1)

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if deriv_pct_change > float(vc_amt):
                sell_code = 1
                reason = "Derivative Value Capture, sell."
            elif pct_change >= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 2:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFP1D_v0_regAggVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if deriv_pct_change > float(vc_amt):
                sell_code = 1
                reason = "Derivative Value Capture, sell."
            elif pct_change <= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC1D_v0_regAggVCSell(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")


        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if deriv_pct_change > float(vc_amt):
                sell_code = 1
                reason = "Derivative Value Capture, sell."
            elif pct_change >= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict


def time_decay_alpha_BFP_v0_regAggVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")        
        vc_amt,risk_pct = config['vc_level'].split("$")


        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if deriv_pct_change > float(vc_amt):
                risk -= float(risk_adj2)
                Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
                if pct_change > Floor_pct:
                    sell_code = 1
                    reason = "Derivative Value Capture, sell."
            elif pct_change <= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
    
        


def time_decay_alpha_BFC_v0_regAggVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")

        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 2:
            if deriv_pct_change > float(vc_amt):
                risk -= float(risk_adj2)
                Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)
                if pct_change < Floor_pct:
                    sell_code = 1
                    reason = "Derivative Value Capture, sell."
            elif pct_change >= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 2:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict
        


def time_decay_alpha_BFP1D_v0_regAggVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
    
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        min_pct_change = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")



        Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} min_value: {min_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if deriv_pct_change > float(vc_amt):
                risk -= float(risk_adj2)
                Floor_pct = ((float(min_value) - float(underlying_open_price))/float(underlying_open_price)) + (vol*risk)
                if pct_change > Floor_pct:
                    sell_code = 1
                    reason = "Derivative Value Capture, sell."
            elif pct_change <= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change > (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def time_decay_alpha_BFC1D_v0_regAggVC(polygon_df, simulation_date, quantity, regression_tgt, vol, config):
    underlying_open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        risk = float(config['standard_risk'])
        if index == 0:
            continue
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        pct_change = ((float(row['underlying_price']) - float(underlying_open_price))/float(underlying_open_price))
        max_pct_change = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price))
        deriv_pct_change = ((float(max_deriv_value) - float(derivative_open_price))/float(derivative_open_price)) * 100

        risk_adj1, risk_adj2 = config['risk_adjustment'].split("$")
        vc_amt,risk_pct = config['vc_level'].split("$")


        Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)

        print(f"vol {vol} risk {risk} Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {underlying_open_price} for {row['ticker']}")
        day_diff = get_day_diff(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        
        if day_diff < 1:
            if deriv_pct_change > float(vc_amt):
                risk -= float(risk_adj2)
                Floor_pct = ((float(max_value) - float(underlying_open_price))/float(underlying_open_price)) - (vol*risk)
                if pct_change < Floor_pct:
                    sell_code = 1
                    reason = "Derivative Value Capture, sell."
            elif pct_change >= regression_tgt:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 2:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= regression_tgt:
                sell_code = 6
                reason = "Hit exit target, sell."
            elif pct_change < (.5*(regression_tgt)):
                sell_code = 5
                reason = "Failed momentum gate, sell."


        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict