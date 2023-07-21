from datetime import datetime, timedelta
import logging
from helpers.helper import get_business_days, polygon_call, calculate_floor_pct  
import numpy as np  

logger = logging.getLogger()
logger.setLevel(logging.INFO)

### TRADING ALGORITHMS ###

def time_decay_alpha_gainers_v0(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    option_open = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .05
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .02)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct += 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100) * quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": polygon_df.iloc[-1]['o']*100, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_ma_v0(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .05
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .02)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct += 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_maP_v0(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.05
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .02)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct -= 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_losers_v0(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.05
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .02)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct -= 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict


### INV ALERTS STRATEGIES ###

def time_decay_alpha_gainers_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    option_open = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct += 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100) * quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": polygon_df.iloc[-1]['o']*100, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_losersC_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    option_open = polygon_df.iloc[0]['o']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct += 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100) * quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": polygon_df.iloc[-1]['o']*100, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_ma_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct += 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_maP_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct -= 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_losers_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct -= 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_gainersP_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct -= 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict


def time_decay_alpha_vdiffC_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = .04
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) - .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct += 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_vdiffP_v0_inv(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        Target_pct = -.03
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + .0125)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > 0.1:
            Floor_pct -= 0.01

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

### RELATIVE VOL FUNCTIONS ###
def time_decay_alpha_vdiffP_v0_rvol(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        strategy_vol_expectation = 1.2
        Target_pct = (row['threeD_stddev50'] * strategy_vol_expectation)
        floor_modifier = (Target_pct * 0.8)
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + floor_modifier)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > Target_pct:
            Floor_pct -= 0.005

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

def time_decay_alpha_vdiffP_v0_rvol(polygon_df, simulation_date, quantity):
    open_price = polygon_df.iloc[0]['underlying_price']
    for index, row in polygon_df.iterrows():
        max_value = polygon_df.iloc[:index]['underlying_price'].max()
        strategy_vol_expectation = 1.2
        Target_pct = (row['threeD_stddev50'] * strategy_vol_expectation)
        floor_modifier = (Target_pct * 0.8)
        pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
        Floor_pct = ((float(max_value) - float(open_price))/float(open_price) + floor_modifier)

        # if type(Floor_pct) == float:
        #     Floor_pct = -0.02
        if pct_change > Target_pct:
            Floor_pct -= 0.005

        # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
        day_diff = get_business_days(simulation_date, row['date'])
        sell_code = 0
        reason = ""
        if day_diff < 2:
            if pct_change <= Floor_pct:
                sell_code = 2
                reason = "Hit exit target, sell."
        elif day_diff > 3:
            sell_code = 2
            reason = "Held through confidence."
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

        if sell_code == 2:
            sell_dict = {"close_price": row['o'], "close_datetime": row['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (row['o']*100)*quantity, "option_symbol": row['ticker'],"reason": reason}
            return sell_dict
        
    sell_dict = {"close_price": polygon_df.iloc[-1]['o'], "close_datetime": polygon_df.iloc[-1]['date'].to_pydatetime(), "quantity": quantity, "contract_cost": (polygon_df.iloc[-1]['o']*100)*quantity, "option_symbol": polygon_df.iloc[-1]['ticker'],"reason": "never sold"}
    return sell_dict

### BET SIZING FUNCTIONS ###

def build_trade(position, available_funds, risk_unit):
    buy_orders = []
    sell_orders = []
    contract_costs = []
    transactions = position['transactions']
    for transaction in transactions:
        # print(type(trade_info))
        # print(position_id)
        # print(trade_info[0])
        # trades = trade_info[0]
        buy_orders.append(transaction['buy_dict'])
        sell_orders.append(transaction['sell_dict'])
        contract_costs.append(transaction['buy_dict']['contract_cost'])
    
    sized_buys, sized_sells = size_trade(contract_costs, buy_orders, sell_orders, available_funds, risk_unit)
    return sized_buys, sized_sells

def size_trade(contract_costs, buy_orders, sell_orders, available_funds, risk_unit):
    target_cost = (risk_unit * available_funds)
    spread_cost = sum(contract_costs)
    if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
        quantities = [1] * len(buy_orders)
    elif spread_cost < (.9*target_cost):
        quantity = add_spread_cost(spread_cost, target_cost)
        quantities = [quantity] * len(buy_orders)
    elif spread_cost > (1.1*target_cost):
        quantities = reduce_spread(contract_costs, target_cost)

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
                    print("size_trade 1")
                    print(e)
                    print(buy_orders)
        except Exception as e:
            print("size_trade 2")
            print(e)
            print(buy_orders)
            return None, None

    return buy_orders, sell_orders
    


def bet_sizer(contracts, available_funds):
    target_cost = (.02 * available_funds)
    # to_stamp = date.strftime("%Y-%m-%d")
    # from_stamp = (date - timedelta(days=2)).strftime("%Y-%m-%d")
    # contracts_details = []

    ##will use down the road
    # for contract in contracts:
    #     polygon_result = polygon_call(contract,from_stamp, to_stamp,30,"minute")
    #     contract['avg_volume'], contract['avg_transactions'] = build_volume_features(polygon_result)

    spread_cost = calculate_spread_cost(contracts)
    contracts, quantity = finalize_trade(contracts, spread_cost, target_cost)
    if contracts != None:
        sized_spread_cost = spread_cost * quantity
    return contracts, quantity

def pull_trading_balance():
    ### This is hardcoded for now, but will be replaced with a call to the tradier API
    return 100000

def calculate_spread_cost(contracts_details):
    cost = 0
    for contract in contracts_details:
        print("CALC")
        print(contract)
        cost += contract['price']
    return cost

def build_volume_features(df):
    avg_volume = df['v'].mean()
    avg_transactions = df['n'].mean()
    return avg_volume, avg_transactions

def finalize_trade(contracts_details, spread_cost, target_cost):
    if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
        return contracts_details, 1
    elif spread_cost > (1.1*target_cost):
        spread2_cost = calculate_spread_cost(contracts_details[0:2])
        if spread2_cost < (1.1*target_cost):
            return contracts_details[0:2], 1
        else:
            single_contract_cost = 100 * contracts_details[0]['price']
            if single_contract_cost > (1.1*target_cost):
                return [], 0
            else:
                return contracts_details[0:1], 1    
    elif spread_cost < (.9*target_cost):
        spread_cost, spread_multiplier = add_spread_cost(spread_cost, target_cost, contracts_details)
        return contracts_details, spread_multiplier
    else:
        print("else")
        return contracts_details, 1
            
def add_spread_cost(spread_cost, target_cost):
    spread_multiplier = 1
    total_cost = spread_cost
    if spread_cost == 0:
        return 0, 0
    else:
        while total_cost <= target_cost:
            spread_multiplier += 1
            total_cost = spread_cost * spread_multiplier
        
        if total_cost > target_cost:
            spread_multiplier -= 1
            total_cost -= spread_cost

        # if total_cost < (.67*target_cost):
        #     # this causes errors and will not work with current formatting
        #     # sized_contracts = contracts_details + contracts_details[0]
        #     spread_multiplier = spread_multiplier
        # else:
        #     sized_contracts = contracts_details * spread_multiplier


    return spread_multiplier

def reduce_spread(contract_costs, target_cost):
    if contract_costs[0] < target_cost:
        if sum(contract_costs[0:2]):
            return [1,1,0]
        else:
            return [1,0,0]
    elif contract_costs[0] > target_cost:
        if sum(contract_costs[1:3]) > target_cost:
            return [0,0,1]
        else:
            return [0,1,1]
        