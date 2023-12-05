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

