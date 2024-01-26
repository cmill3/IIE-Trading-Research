from datetime import datetime, timedelta
import logging
from helpers.helper import get_day_diff
from helpers.strategy_helper import build_trade_analytics
import numpy as np  
import math
import ast

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def tda_PUT_3D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = float(target_pct)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


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
            if pct_change < (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change < target_pct:
                Floor_pct = (.9*underlying_gain)

            if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change <= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_3D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = float(target_pct)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


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
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change >= target_pct:
                if current_weekday == 4 and hour >= 10:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_1D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    target_pct = float(target_pct)
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change <= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_1D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    target_pct = float(target_pct)
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        
        if pct_change > (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change > target_pct:
            Floor_pct = (.75*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change >= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."
            elif hour == 15:
                sell_code = 7
                reason = "End of day, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_3D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = float(target_pct)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 3:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change < (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change < target_pct:
                Floor_pct = (.9*underlying_gain)

            if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change <= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_3D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = float(target_pct)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 3:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change >= target_pct:
                if current_weekday == 4 and hour >= 10:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_1D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    target_pct = float(target_pct)
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change <= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_1D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    target_pct = float(target_pct)
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > int(config['vc_level']):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        
        if pct_change > (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change > target_pct:
            Floor_pct = (.75*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change >= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_3D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = float(target_pct)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour

        vc1,vc2,vcAMT = config['vc_level'].split("$")
        if deriv_pct_change > int(vc1):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        elif deriv_pct_change > int(vc2):
            sell_code = "VC2Sell"
            Floor_pct = (underlying_gain * float(vcAMT))
            if pct_change >= Floor_pct:
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
                return sell_dict


        

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 3:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change < (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change < target_pct:
                Floor_pct = (.9*underlying_gain)

            if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change <= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_3D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    target_pct = float(target_pct)
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour

        vc1,vc2,vcAMT = config['vc_level'].split("$")
        if deriv_pct_change > int(vc1):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        elif deriv_pct_change > int(vc2):
            sell_code = "VC2Sell"
            Floor_pct = (underlying_gain * float(vcAMT))
            if pct_change <= Floor_pct:
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
                return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 3:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 3:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change >= target_pct:
                if current_weekday == 4 and hour >= 10:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_1D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    target_pct = float(target_pct)
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour

        vc1,vc2,vcAMT = config['vc_level'].split("$")
        if deriv_pct_change > int(vc1):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        elif deriv_pct_change > int(vc2):
            sell_code = "VC2Sell"
            Floor_pct = (underlying_gain * float(vcAMT))
            if pct_change >= Floor_pct:
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
                return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change >= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change <= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change >= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_1D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    target_pct = float(target_pct)
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour

        vc1,vc2,vcAMT = config['vc_level'].split("$")
        if deriv_pct_change > int(vc1):
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        elif deriv_pct_change > int(vc2):
            sell_code = "VC2Sell"
            Floor_pct = (underlying_gain * float(vcAMT))
            if pct_change <= Floor_pct:
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
                sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
                return sell_dict

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > (2*target_pct):
                Floor_pct = (.95*underlying_gain)
            elif pct_change > target_pct:
                Floor_pct = (.9*underlying_gain)

            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change >= target_pct:
                if current_weekday == 4 and hour >= 11:
                    Floor_pct = (.99*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 8
                        reason = "Hit exit target, sell."
                else:
                    Floor_pct = (.9*underlying_gain)
                    if pct_change <= Floor_pct:
                        sell_code = 6
                        reason = "Hit exit target, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_3D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > 300:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict
        
        if pct_change < (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change < target_pct:
            Floor_pct = (.75*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 2:
            if deriv_pct_change <= config['floor_value']:
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
            elif pct_change <= target_pct:
                Floor_pct = (.9*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."
            elif hour == 15:
                sell_code = 7
                reason = "End of day, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_3D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

        if deriv_pct_change > 300:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change > (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change > target_pct:
            Floor_pct = (.75*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 2:
            if deriv_pct_change <= config['floor_value']:
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
            elif pct_change >= target_pct:
                Floor_pct = (.9*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."
            elif hour == 15:
                sell_code = 7
                reason = "End of day, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_1D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
        if deriv_pct_change > 300:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change < (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change < target_pct:
            Floor_pct = (.75*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if deriv_pct_change <= config['floor_value']:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= target_pct:
                Floor_pct = (.9*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change > (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."
            elif hour == 15:
                sell_code = 7
                reason = "End of day, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_1D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > 300:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change > (2*target_pct):
            Floor_pct = (.9*underlying_gain)
        elif pct_change > target_pct:
            Floor_pct = (.75*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


        sell_code = 0
        reason = ""
        if day_diff < 1:
            if deriv_pct_change <= config['floor_value']:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff == 1:
            if pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= target_pct:
                Floor_pct = (.9*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."
            elif hour == 15:
                sell_code = 7
                reason = "End of day, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_3D_CDVOL(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        Floor_pct -= underlying_gain
        hour = row['date'].hour
        # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

        if deriv_pct_change > config['vc_level']:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change < target_pct:
            Floor_pct = (.9*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 2:
            if underlying_gain > Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= target_pct:
                Floor_pct = (.95*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change >= (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_3D_CDVOL(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (-vol * config['volatility_threshold'])
        Floor_pct += underlying_gain
        hour = row['date'].hour
        # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

        if deriv_pct_change > config['vc_level']:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change > target_pct:
            Floor_pct = (.9*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 2:
            if underlying_gain < Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= target_pct:
                Floor_pct = (.95*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_PUT_1D_CDVOL(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        min_value = polygon_df.iloc[:index]['underlying_price'].min()
        underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = (vol * config['volatility_threshold'])
        Floor_pct -= underlying_gain
        hour = row['date'].hour

        if deriv_pct_change > config['vc_level']:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change < target_pct:
            Floor_pct = (.9*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 2:
            if underlying_gain > Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 3:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff >= 2:
            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change > Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change <= target_pct:
                Floor_pct = (.95*underlying_gain)
                if pct_change >= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change >= (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict

def tda_CALL_1D_CDVOL(polygon_df, simulation_date, quantity, config, target_pct, vol):
    open_price = polygon_df.iloc[0]['underlying_price']
    derivative_open_price = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        if index == 0:
            continue
        max_deriv_value = polygon_df.iloc[:index]['o'].max()
        deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        hour = row['date'].hour
        Floor_pct = (-vol * config['volatility_threshold'])
        Floor_pct += underlying_gain

        if deriv_pct_change > config['vc_level']:
            sell_code = "VCSell"
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
            return sell_dict

        if pct_change > target_pct:
            Floor_pct = (.9*underlying_gain)

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

        sell_code = 0
        reason = ""
        if day_diff < 1:
            if underlying_gain < Floor_pct:
                sell_code = 2
                reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
        elif day_diff > 1:
            sell_code = 3
            reason = "Held through confidence."
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
            return sell_dict
        elif day_diff ==1 :
            if hour == 15 or (current_weekday == 4 and hour >= 12):
                sell_code = 7
                reason = "End of day, sell."
            elif pct_change < Floor_pct:
                sell_code = 4
                reason = "Hit point of no confidence, sell."
            elif pct_change >= target_pct:
                Floor_pct = (.95*underlying_gain)
                if pct_change <= Floor_pct:
                    sell_code = 6
                    reason = "Hit exit target, sell."
            elif pct_change < (.5*(target_pct)):
                sell_code = 5
                reason = "Failed momentum gate, sell."

        if sell_code != 0:
            sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,sell_code)
            return sell_dict
        
        
    sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
    return sell_dict