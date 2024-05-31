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


# def tda_PUT_3D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     target_pct = float(target_pct)
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
        

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change < (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change < target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 10:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_1D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_1D_stdcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
        
#         if pct_change > (2*target_pct):
#             Floor_pct = (.9*underlying_gain)
#         elif pct_change > target_pct:
#             Floor_pct = (.75*underlying_gain)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."
#             elif hour == 15:
#                 sell_code = 7
#                 reason = "End of day, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_3D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     target_pct = float(target_pct)
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
        

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         # if day_diff < 3:
#         #     if pct_change >= Floor_pct:
#         #         sell_code = 2
#         #         reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         # el
#         if day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change < (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change < target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         # if day_diff < 3:
#         #     if pct_change <= Floor_pct:
#         #         sell_code = 2
#         #         reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         # el
#         if day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 10:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_1D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         # if day_diff < 1:
#         #     if pct_change >= Floor_pct:
#         #         sell_code = 2
#         #         reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         # el
#         if day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_1D_stdclsAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
        

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         # if day_diff < 1:
#         #     if pct_change <= Floor_pct:
#         #         sell_code = 2
#         #         reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         # el
#         if day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_3D_simple(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     target_pct = float(target_pct)
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
        

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""

#         if day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change < (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change < target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_simple(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""

#         if day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 10:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_1D_simple(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_1D_simple(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(config['vc_level']):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict


#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_3D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     target_pct = float(target_pct)
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

#         vc1,vc2,vcAMT = config['vc_level'].split("$")
#         if deriv_pct_change > int(vc1):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
#         elif deriv_pct_change > int(vc2):
#             sell_code = "VC2Sell"
#             Floor_pct = (underlying_gain * float(vcAMT))
#             if pct_change >= Floor_pct:
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#                 sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#                 return sell_dict


        

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change < (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change < target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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

#         vc1,vc2,vcAMT = config['vc_level'].split("$")
#         if deriv_pct_change > int(vc1):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
#         elif deriv_pct_change > int(vc2):
#             sell_code = "VC2Sell"
#             Floor_pct = (underlying_gain * float(vcAMT))
#             if pct_change <= Floor_pct:
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#                 sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#                 return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if (day_diff == 3 and hour == 15) or (current_weekday == 4 and hour > 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 10:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_1D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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

#         vc1,vc2,vcAMT = config['vc_level'].split("$")
#         if deriv_pct_change > int(vc1):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
#         elif deriv_pct_change > int(vc2):
#             sell_code = "VC2Sell"
#             Floor_pct = (underlying_gain * float(vcAMT))
#             if pct_change >= Floor_pct:
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#                 sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#                 return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if pct_change >= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change <= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change >= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_1D_VCcls(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     target_pct = float(target_pct)
#     derivative_open_price = polygon_df.iloc[0]['o']
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

#         vc1,vc2,vcAMT = config['vc_level'].split("$")
#         if deriv_pct_change > int(vc1):
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
#         elif deriv_pct_change > int(vc2):
#             sell_code = "VC2Sell"
#             Floor_pct = (underlying_gain * float(vcAMT))
#             if pct_change <= Floor_pct:
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#                 sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#                 return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if pct_change <= Floor_pct:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > (2*target_pct):
#                 Floor_pct = (.95*underlying_gain)
#             elif pct_change > target_pct:
#                 Floor_pct = (.9*underlying_gain)

#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change >= target_pct:
#                 if current_weekday == 4 and hour >= 11:
#                     Floor_pct = (.99*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 8
#                         reason = "Hit exit target, sell."
#                 else:
#                     Floor_pct = (.9*underlying_gain)
#                     if pct_change <= Floor_pct:
#                         sell_code = 6
#                         reason = "Hit exit target, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_3D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > 300:
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict
        
#         if pct_change < (2*target_pct):
#             Floor_pct = (.9*underlying_gain)
#         elif pct_change < target_pct:
#             Floor_pct = (.75*underlying_gain)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if deriv_pct_change <= config['floor_value']:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff >= 2:
#             if pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= target_pct:
#                 Floor_pct = (.9*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."
#             elif hour == 15:
#                 sell_code = 7
#                 reason = "End of day, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

#         if deriv_pct_change > 300:
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict

#         if pct_change > (2*target_pct):
#             Floor_pct = (.9*underlying_gain)
#         elif pct_change > target_pct:
#             Floor_pct = (.75*underlying_gain)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 2:
#             if deriv_pct_change <= config['floor_value']:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff >= 2:
#             if pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= target_pct:
#                 Floor_pct = (.9*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."
#             elif hour == 15:
#                 sell_code = 7
#                 reason = "End of day, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_1D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))
#         if deriv_pct_change > 300:
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict

#         if pct_change < (2*target_pct):
#             Floor_pct = (.9*underlying_gain)
#         elif pct_change < target_pct:
#             Floor_pct = (.75*underlying_gain)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if deriv_pct_change <= config['floor_value']:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change > Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change <= target_pct:
#                 Floor_pct = (.9*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change > (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."
#             elif hour == 15:
#                 sell_code = 7
#                 reason = "End of day, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_1D_derivVOL(polygon_df, simulation_date, quantity, config, target_pct, vol, standard_risk):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
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
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > 300:
#             sell_code = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,sell_code)  
#             return sell_dict

#         if pct_change > (2*target_pct):
#             Floor_pct = (.9*underlying_gain)
#         elif pct_change > target_pct:
#             Floor_pct = (.75*underlying_gain)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])


#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if deriv_pct_change <= config['floor_value']:
#                 sell_code = 2
#                 reason = f"Breached floor pct, sell. {pct_change} {Floor_pct}"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if pct_change < Floor_pct:
#                 sell_code = 4
#                 reason = "Hit point of no confidence, sell."
#             elif pct_change >= target_pct:
#                 Floor_pct = (.9*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."
#             elif hour == 15:
#                 sell_code = 7
#                 reason = "End of day, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_3D_CDVOLAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     isVC = False
#     Floor_pct = (-vol * config['volatility_threshold'])
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         min_value = polygon_df.iloc[:index]['underlying_price'].min()
#         underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         # Floor_pct -= underlying_gain
#         hour = row['date'].hour
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

#         # vc1,vc2,pct = config['vc_level'].split('+')
#         if deriv_pct_change > config['vc_level']:
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         # elif deriv_pct_change > float(vc2):
#         #     isVC = True
#         #     Floor_pct = underlying_gain * float(pct)


#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change > Floor_pct:
#                 if isVC:
#                     reason = "VC Sell Early"
#                 else:
#                     sell_code = 2
#                     reason = f"Breached floor pct"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 if isVC:
#                     reason = "VC Sell EOD"
#                 else:
#                     reason = "Hit point of no confidence, sell."
#             elif pct_change <= target_pct:
#                 Floor_pct = (.95*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change >= (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_CDVOLAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     isVC = False
#     Floor_pct = (-vol * config['volatility_threshold'])
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         # Floor_pct += underlying_gain
#         hour = row['date'].hour
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))

#         # vc1,vc2,pct = config['vc_level'].split('+')
#         if deriv_pct_change > config['vc_level']:
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         # elif deriv_pct_change > float(vc2):
#         #     isVC = True
#         #     Floor_pct = underlying_gain * float(pct)

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change < Floor_pct:
#                 if isVC:
#                     reason = "VC Sell Early"
#                 else:
#                     sell_code = 2
#                     reason = f"Breached floor pct"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 if isVC:
#                     reason = "VC Sell EOD"
#                 else:
#                     reason = "Hit point of no confidence, sell."
#             elif pct_change >= target_pct:
#                 Floor_pct = (.95*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_1D_CDVOLAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     isVC = False
#     Floor_pct = (-vol * config['volatility_threshold'])
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         min_value = polygon_df.iloc[:index]['underlying_price'].min()
#         underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         # Floor_pct -= underlying_gain
#         hour = row['date'].hour

#         # vc1,vc2,pct = config['vc_level'].split('+')
#         if deriv_pct_change > config['vc_level']:
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         # elif deriv_pct_change > float(vc2):
#         #     isVC = True
#         #     Floor_pct = underlying_gain * float(pct)


#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if pct_change > Floor_pct:
#                 if isVC:
#                     reason = "VC Sell Early"
#                 else:
#                     sell_code = 2
#                     reason = f"Breached floor pct"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1:
#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 if isVC:
#                     reason = "VC Sell EOD"
#                 else:
#                     reason = "Hit point of no confidence, sell."
#             elif pct_change <= target_pct:
#                 Floor_pct = (.95*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change >= (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_1D_CDVOLAGG(polygon_df, simulation_date, quantity, config, target_pct, vol):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     isVC = False
#     Floor_pct = (-vol * config['volatility_threshold'])
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         hour = row['date'].hour
#         Floor_pct = ((-vol * config['volatility_threshold'])*.7)
#         # Floor_pct += underlying_gain

#         # vc1,vc2,pct = config['vc_level'].split('+')
#         if deriv_pct_change > config['vc_level']:
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         # elif deriv_pct_change > float(vc2):
#         #     isVC = True
#         #     Floor_pct = underlying_gain * float(pct)


#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 1:
#             if pct_change < Floor_pct:
#                 if isVC:
#                     reason = "VC Sell Early"
#                 else:
#                     sell_code = 2
#                     reason = f"Breached floor pct"
#         elif day_diff > 1:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 1 :
#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 if isVC:
#                     reason = "VC Sell EOD"
#                 else:
#                     reason = "Hit point of no confidence, sell."
#             elif pct_change >= target_pct:
#                 Floor_pct = (.95*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_PUT_3D_CDVOLVARVC(polygon_df, simulation_date, quantity, config, target_pct, vol, order_num):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     Floor_pct = (-vol * config['volatility_threshold'])
#     isVC = False
#     if order_num > 4:
#         order_num = 4
#     vc_values = config['vc_level'].split('+')
#     vc_config = {
#         0: vc_values[0],
#         1: vc_values[1],
#         2: vc_values[2],
#         3: vc_values[3]
#     }
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         min_value = polygon_df.iloc[:index]['underlying_price'].min()
#         underlying_gain = ((float(min_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         # Floor_pct -= underlying_gain
#         hour = row['date'].hour
#         # Floor_pct = ((float(min_value) - float(open_price))/float(open_price)) + (standard_risk + (-1*config['risk_adjustment']))

#         if deriv_pct_change > int(vc_config[order_num]):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict


#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change > Floor_pct:
#                 if isVC:
#                     reason = "VC Sell Early"
#                 else:
#                     sell_code = 2
#                     reason = f"Breached floor pct"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change > Floor_pct:
#                 sell_code = 4
#                 if isVC:
#                     reason = "VC Sell EOD"
#                 else:
#                     reason = "Hit point of no confidence, sell."
#             elif pct_change <= target_pct:
#                 Floor_pct = (.95*underlying_gain)
#                 if pct_change >= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change >= (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict

# def tda_CALL_3D_CDVOLVARVC(polygon_df, simulation_date, quantity, config, target_pct, vol,order_num):
#     open_price = polygon_df.iloc[0]['underlying_price']
#     derivative_open_price = polygon_df.iloc[0]['o']
#     isVC = False
#     Floor_pct = (-vol * config['volatility_threshold'])
#     if order_num > 4:
#         order_num = 4
#     vc_values = config['vc_level'].split('+')
#     vc_config = {
#         0: vc_values[0],
#         1: vc_values[1],
#         2: vc_values[2],
#         3: vc_values[3]
#     }
#     for index, row in polygon_df.iterrows():
#         if index == 0:
#             continue
#         max_deriv_value = polygon_df.iloc[:index]['o'].max()
#         deriv_pct_change = ((max_deriv_value - float(derivative_open_price))/float(derivative_open_price))*100
#         max_value = polygon_df.iloc[:index]['underlying_price'].max()
#         underlying_gain = ((float(max_value) - float(open_price))/float(open_price))
#         pct_change = ((float(row['underlying_price']) - float(open_price))/float(open_price))
#         # Floor_pct += underlying_gain
#         hour = row['date'].hour
#         # Floor_pct = ((float(max_value) - float(open_price))/float(open_price)) - (float(standard_risk) + (-1*config['risk_adjustment']))


#         if deriv_pct_change > int(vc_config[order_num]):
#             reason = "VCSell"
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict

#         # print(f"Floor_pct: {Floor_pct} max_value: {max_value} pct_change: {pct_change} current_price: {row['underlying_price']} purchase_price: {open_price} for {row['ticker']}")
#         day_diff, current_weekday = get_day_diff(simulation_date, row['date'])

#         sell_code = 0
#         reason = ""
#         if day_diff < 3:
#             if pct_change < Floor_pct:
#                 if isVC:
#                     reason = "VC Sell Early"
#                 else:
#                     sell_code = 2
#                     reason = f"Breached floor pct"
#         elif day_diff > 3:
#             sell_code = 3
#             reason = "Held through confidence."
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,reason)  
#             return sell_dict
#         elif day_diff == 3:
#             if hour == 15 or (current_weekday == 4 and hour >= 12):
#                 sell_code = 7
#                 reason = "End of day, sell."
#             elif pct_change < Floor_pct:
#                 sell_code = 4
#                 if isVC:
#                     reason = "VC Sell EOD"
#                 else:
#                     reason = "Hit point of no confidence, sell."
#             elif pct_change >= target_pct:
#                 Floor_pct = (.95*underlying_gain)
#                 if pct_change <= Floor_pct:
#                     sell_code = 6
#                     reason = "Hit exit target, sell."
#             elif pct_change < (.5*(target_pct)):
#                 sell_code = 5
#                 reason = "Failed momentum gate, sell."

#         if sell_code != 0:
#             sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,index,quantity,reason)
#             return sell_dict
        
#     sell_dict = build_trade_analytics(row,polygon_df,derivative_open_price,len(polygon_df)-1,quantity,"never sold")
#     return sell_dict