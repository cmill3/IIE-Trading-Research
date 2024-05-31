# from datetime import datetime, timedelta
# import logging
# from helpers.helper import get_day_diff
# from helpers.strategy_helper import build_trade_analytics_RMF
# import numpy as np  
# import math
# import ast

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)


# def tda_PUT_3D_RMF(polygon_df, simulation_date, quantity, config, target_pct, rm_target_pct, vol, aggregate_classification):
#     Floor_pct = (vol * config['volatility_threshold'])
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     hi_target, lo_target, hilo_score = sort_targets(float(target_pct), float(rm_target_pct),aggregate_classification)
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         min_value = polygon_df.iloc[:index]['underlying_price'].min()
#         underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         hour = row['date'].hour
#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             reason = "VCSell"
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason,aggregate_classification, hilo_score)
#             return sell_dict
#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         if hilo_score == "11":
#             sell_code,reason = manage_trade_11_PUT3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "12":
#             sell_code,reason = manage_trade_10_PUT3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "21":
#             sell_code,reason = manage_trade_10_PUT3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)

#         if sell_code != 0:
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason,aggregate_classification, hilo_score)
#             return sell_dict
        
#     sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold",aggregate_classification, hilo_score)
#     return sell_dict

# def tda_CALL_3D_RMF(polygon_df, simulation_date, quantity, config, target_pct, rm_target_pct, vol, aggregate_classification):
#     Floor_pct = -(vol * config['volatility_threshold'])
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     hi_target, lo_target, hilo_score = sort_targets(float(target_pct), float(rm_target_pct),aggregate_classification)
#     target_pct = float(target_pct)
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = (-vol * config['volatility_threshold'])
#         hour = row['date'].hour

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code,aggregate_classification, hilo_score)
#             return sell_dict
#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         if hilo_score == "11":
#             sell_code, reason = manage_trade_11_CALL3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "12":
#             sell_code, reason = manage_trade_10_CALL3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "21":
#             sell_code, reason = manage_trade_10_CALL3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)

#         if sell_code != 0:
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code,aggregate_classification, hilo_score)
#             return sell_dict
        
#     sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold",aggregate_classification, hilo_score)
#     return sell_dict

# def tda_PUT_1D_RMF(polygon_df, simulation_date, quantity, config, target_pct, rm_target_pct, vol, aggregate_classification):
#     Floor_pct = (vol * config['volatility_threshold'])
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
#     hi_target, lo_target, hilo_score = sort_targets(float(target_pct), float(rm_target_pct),aggregate_classification)
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         min_value = polygon_df.iloc[:index]['underlying_price'].min()
#         underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = (vol * config['volatility_threshold'])
#         hour = row['date'].hour

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code,aggregate_classification, hilo_score)
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         if hilo_score == "11":
#             sell_code, reason  = manage_trade_11_PUT1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "12":
#             sell_code, reason  = manage_trade_10_PUT1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "21":
#             sell_code, reason  = manage_trade_10_PUT1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
        
#         if sell_code != 0:
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code,aggregate_classification, hilo_score)
#             return sell_dict
        
#     sell_dict =   (row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold",aggregate_classification, hilo_score)
#     return sell_dict

# def tda_CALL_1D_RMF(polygon_df, simulation_date, quantity, config, target_pct, rm_target_pct, vol, aggregate_classification):
#     Floor_pct = -(vol * config['volatility_threshold'])
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
#     hi_target, lo_target, hilo_score = sort_targets(float(target_pct), float(rm_target_pct),aggregate_classification)
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         Floor_pct = (-vol * config['volatility_threshold'])
#         hour = row['date'].hour

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code,aggregate_classification, hilo_score)
#             return sell_dict
        
#         if pct_change > (2*target_pct):
#             Floor_pct = (.9*underlying_gain)
#         elif pct_change > target_pct:
#             Floor_pct = (.75*underlying_gain)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         if hilo_score == "11":
#             sell_code, reason  = manage_trade_11_CALL1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "12":
#             sell_code, reason  = manage_trade_10_CALL1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)
#         elif hilo_score == "21":
#             sell_code, reason  = manage_trade_10_CALL1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct)

#         if sell_code != 0:
#             sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code,aggregate_classification, hilo_score)
#             return sell_dict
        
#     sell_dict = build_trade_analytics_RMF(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold",aggregate_classification, hilo_score)
#     return sell_dict


# def sort_targets(target_pct, rm_target_pct, aggregate_classification):
#     rm_score = aggregate_classification[1]
#     fix_score = aggregate_classification[0]
#     if target_pct > rm_target_pct:
#         return target_pct, rm_target_pct, f"{fix_score}{rm_score}"
#     else:
#         return rm_target_pct, target_pct, f"{rm_score}{fix_score}"
    

# def manage_trade_11_CALL3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if underlying_gain > hi_target:
#             Floor_pct = (.85*underlying_gain)
#         elif underlying_gain > lo_target:
#             Floor_pct = (.7*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 3:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff >= 2:
#         if underlying_gain > hi_target:
#             Floor_pct = (.9*underlying_gain)
#         elif underlying_gain > lo_target:
#             Floor_pct = (.75*underlying_gain)

#         if pct_change < Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change >= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change < (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_11_PUT3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if underlying_gain < hi_target:
#             Floor_pct = (.85*underlying_gain)
#         elif underlying_gain < lo_target:
#             Floor_pct = (.7*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 3:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff >= 2:
#         if underlying_gain < hi_target:
#             Floor_pct = (.9*underlying_gain)
#         elif underlying_gain < lo_target:
#             Floor_pct = (.75*underlying_gain)

#         if pct_change > Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change <= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change > (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_11_CALL1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 1:
#         if underlying_gain > hi_target:
#             Floor_pct = (.85*underlying_gain)
#         elif underlying_gain > lo_target:
#             Floor_pct = (.7*underlying_gain)
#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 1:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff == 1:
#         if underlying_gain > hi_target:
#             Floor_pct = (.9*underlying_gain)
#         elif underlying_gain > lo_target:
#             Floor_pct = (.75*underlying_gain)

#         if pct_change < Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change >= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change < (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_11_PUT1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if underlying_gain < hi_target:
#             Floor_pct = (.85*underlying_gain)
#         elif underlying_gain < lo_target:
#             Floor_pct = (.7*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 3:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff >= 2:
#         if underlying_gain < hi_target:
#             Floor_pct = (.9*underlying_gain)
#         elif underlying_gain < lo_target:
#             Floor_pct = (.75*underlying_gain)

#         if pct_change > Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change <= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change > (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_10_CALL3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if underlying_gain > lo_target:
#             Floor_pct = (.9*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 3:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff >= 2:
#         if underlying_gain > lo_target:
#             Floor_pct = (.95*underlying_gain)

#         if pct_change < Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change >= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change < (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_10_PUT3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if underlying_gain < lo_target:
#             Floor_pct = (.9*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 3:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff >= 2:
#         if underlying_gain < lo_target:
#             Floor_pct = (.95*underlying_gain)

#         if pct_change > Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change <= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change > (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_10_CALL1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 1:
#         if underlying_gain > lo_target:
#             Floor_pct = (.9*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 1:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff == 1:
#         if underlying_gain > lo_target:
#             Floor_pct = (.95*underlying_gain)

#         if pct_change < Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change >= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change < (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# def manage_trade_10_PUT1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
#     sell_code = 0
#     reason = ""
#     if day_diff < 2:
#         if underlying_gain < lo_target:
#             Floor_pct = (.9*underlying_gain)

#         if pct_change <= Floor_pct:
#             sell_code = 2
#             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#     elif day_diff > 3:
#         sell_code = 3
#         reason = "Held through confidence."
#         return sell_code, reason
#     elif day_diff >= 2:
#         if underlying_gain < lo_target:
#             Floor_pct = (.95*underlying_gain)

#         if pct_change > Floor_pct:
#             sell_code = 4
#             reason = "Hit point of no confidence, sell."
#         elif pct_change <= lo_target:
#             if current_weekday == 4: 
#                 if hour < 11:
#                     Floor_pct = (.9*underlying_gain)
#                 elif hour >= 11:
#                     Floor_pct = (.96*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 8
#                     reason = "Hit exit target, sell."
#             else:
#                 Floor_pct = (.8*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#         elif pct_change > (.5*(lo_target)):
#             sell_code = 5
#             reason = "Failed momentum gate, sell."
#         elif hour == 15 or (current_weekday == 4 and hour >= 14):
#             sell_code = 7
#             reason = "End of day, sell."
#     return sell_code, reason

# # def manage_trade_01_CALL3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
# #     sell_code = 0
# #     reason = ""
# #     if day_diff < 2:
# #         if underlying_gain > hi_target:
# #             Floor_pct = (.85*underlying_gain)
# #         elif underlying_gain > lo_target:
# #             Floor_pct = (.7*underlying_gain)

# #         if pct_change <= Floor_pct:
# #             sell_code = 2
# #             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
# #     elif day_diff > 3:
# #         sell_code = 3
# #         reason = "Held through confidence."
# #         return sell_code, reason
# #     elif day_diff >= 2:
# #         if underlying_gain > hi_target:
# #             Floor_pct = (.9*underlying_gain)
# #         elif underlying_gain > lo_target:
# #             Floor_pct = (.75*underlying_gain)

# #         if pct_change < Floor_pct:
# #             sell_code = 4
# #             reason = "Hit point of no confidence, sell."
# #         elif pct_change >= lo_target:
# #             if current_weekday == 4: 
# #                 if hour < 11:
# #                     Floor_pct = (.9*underlying_gain)
# #                 elif hour >= 11:
# #                     Floor_pct = (.96*underlying_gain)
# #                 if pct_change <= Floor_pct:
# #                     sell_code = 8
# #                     reason = "Hit exit target, sell."
# #             else:
# #                 Floor_pct = (.8*underlying_gain)
# #                 if pct_change <= Floor_pct:
# #                     sell_code = 6
# #                     reason = "Hit exit target, sell."
# #         elif pct_change < (.5*(lo_target)):
# #             sell_code = 5
# #             reason = "Failed momentum gate, sell."
# #         elif hour == 15 or (current_weekday == 4 and hour >= 14):
# #             sell_code = 7
# #             reason = "End of day, sell."
# #     return sell_code, reason

# # def manage_trade_01_PUT3D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
# #     sell_code = 0
# #     reason = "" 
# #     if day_diff < 2:
# #         if underlying_gain < hi_target:
# #             Floor_pct = (.85*underlying_gain)
# #         elif underlying_gain < lo_target:
# #             Floor_pct = (.7*underlying_gain)

# #         if pct_change <= Floor_pct:
# #             sell_code = 2
# #             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
# #     elif day_diff > 3:
# #         sell_code = 3
# #         reason = "Held through confidence."
# #         return sell_code, reason
# #     elif day_diff >= 2:
# #         if underlying_gain < hi_target:
# #             Floor_pct = (.9*underlying_gain)
# #         elif underlying_gain < lo_target:
# #             Floor_pct = (.75*underlying_gain)

# #         if pct_change > Floor_pct:
# #             sell_code = 4
# #             reason = "Hit point of no confidence, sell."
# #         elif pct_change <= lo_target:
# #             if current_weekday == 4: 
# #                 if hour < 11:
# #                     Floor_pct = (.9*underlying_gain)
# #                 elif hour >= 11:
# #                     Floor_pct = (.96*underlying_gain)
# #                 if pct_change >= Floor_pct:
# #                     sell_code = 8
# #                     reason = "Hit exit target, sell."
# #             else:
# #                 Floor_pct = (.8*underlying_gain)
# #                 if pct_change >= Floor_pct:
# #                     sell_code = 6
# #                     reason = "Hit exit target, sell."
# #         elif pct_change > (.5*(lo_target)):
# #             sell_code = 5
# #             reason = "Failed momentum gate, sell."
# #         elif hour == 15 or (current_weekday == 4 and hour >= 14):
# #             sell_code = 7
# #             reason = "End of day, sell."
# #     return sell_code, reason

# # def manage_trade_01_CALL1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
# #     sell_code = 0
# #     reason = ""
# #     if day_diff < 1:
# #         if underlying_gain > hi_target:
# #             Floor_pct = (.85*underlying_gain)
# #         elif underlying_gain > lo_target:
# #             Floor_pct = (.7*underlying_gain)

# #         if pct_change <= Floor_pct:
# #             sell_code = 2
# #             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
# #     elif day_diff > 1:
# #         sell_code = 3
# #         reason = "Held through confidence."
# #         return sell_code, reason
# #     elif day_diff == 1:
# #         if underlying_gain > hi_target:
# #             Floor_pct = (.9*underlying_gain)
# #         elif underlying_gain > lo_target:
# #             Floor_pct = (.75*underlying_gain)

# #         if pct_change < Floor_pct:
# #             sell_code = 4
# #             reason = "Hit point of no confidence, sell."
# #         elif pct_change >= lo_target:
# #             if current_weekday == 4: 
# #                 if hour < 11:
# #                     Floor_pct = (.9*underlying_gain)
# #                 elif hour >= 11:
# #                     Floor_pct = (.96*underlying_gain)
# #                 if pct_change <= Floor_pct:
# #                     sell_code = 8
# #                     reason = "Hit exit target, sell."
# #             else:
# #                 Floor_pct = (.8*underlying_gain)
# #                 if pct_change <= Floor_pct:
# #                     sell_code = 6
# #                     reason = "Hit exit target, sell."
# #         elif pct_change < (.5*(lo_target)):
# #             sell_code = 5
# #             reason = "Failed momentum gate, sell."
# #         elif hour == 15 or (current_weekday == 4 and hour >= 14):
# #             sell_code = 7
# #             reason = "End of day, sell."
# #     return sell_code, reason

# # def manage_trade_01_PUT1D(hi_target, lo_target, underlying_gain, pct_change, current_weekday, hour, day_diff, Floor_pct):
# #     sell_code = 0
# #     reason = ""
# #     if day_diff < 2:
# #         if underlying_gain < hi_target:
# #             Floor_pct = (.85*underlying_gain)
# #         elif underlying_gain < lo_target:
# #             Floor_pct = (.7*underlying_gain)

# #         if pct_change <= Floor_pct:
# #             sell_code = 2
# #             reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
# #     elif day_diff > 3:
# #         sell_code = 3
# #         reason = "Held through confidence."
# #         return sell_code, reason
# #     elif day_diff >= 2:
# #         if underlying_gain < hi_target:
# #             Floor_pct = (.9*underlying_gain)
# #         elif underlying_gain < lo_target:
# #             Floor_pct = (.75*underlying_gain)

# #         if pct_change > Floor_pct:
# #             sell_code = 4
# #             reason = "Hit point of no confidence, sell."
# #         elif pct_change <= lo_target:
# #             if current_weekday == 4: 
# #                 if hour < 11:
# #                     Floor_pct = (.9*underlying_gain)
# #                 elif hour >= 11:
# #                     Floor_pct = (.96*underlying_gain)
# #                 if pct_change >= Floor_pct:
# #                     sell_code = 8
# #                     reason = "Hit exit target, sell."
# #             else:
# #                 Floor_pct = (.8*underlying_gain)
# #                 if pct_change >= Floor_pct:
# #                     sell_code = 6
# #                     reason = "Hit exit target, sell."
# #         elif pct_change > (.5*(lo_target)):
# #             sell_code = 5
# #             reason = "Failed momentum gate, sell."
# #         elif hour == 15 or (current_weekday == 4 and hour >= 14):
# #             sell_code = 7
# #             reason = "End of day, sell."
# #     return sell_code, reason