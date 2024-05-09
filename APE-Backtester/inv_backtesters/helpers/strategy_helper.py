# import math
# import pandas as pd
# from datetime import datetime
# from helpers.backtest_functions import simulate_trades_invalert_SINGLE


# ### BET SIZING FUNCTIONS ###
# def build_trade(position, risk_unit,put_adjustment,portfolio_cash,config):
#     print("BUILDING TRADE")
#     print(position)
#     buy_orders = []
#     sell_orders = []
#     contract_costs = []
#     # spread_start, spread_end = config['spread_search'].split(":")
#     # transactions = position['transactions'][int(spread_start):int(spread_end)]
#     transactions = simulate_trades_invalert_SINGLE(position,config,portfolio_cash)
#     for transaction in position['transactions']:
#         # print(type(trade_info))
#         # print(position_id)
#         # print(trade_info[0])
#         try:
#             transaction['sell_info']['close_trade_dt'] = transaction['close_trade_dt']
#             buy_orders.append(transaction['buy_info'])
#             sell_orders.append(transaction['sell_info'])
#             contract_costs.append({"option_symbol":transaction['buy_info']['option_symbol'],"contract_cost":transaction['buy_info']['contract_cost']})
#         except Exception as e:
#             print(f"ERROR in build_trade f{e}")
#             print(e)
#             print(transaction)
#             print(position)
#             return [], []

#     sized_buys, sized_sells = bet_sizer(contract_costs, buy_orders, sell_orders, risk_unit,portfolio_cash,config,position['open_datetime'])
#     if sized_buys == None:
#         # sized_buys,sized_sells = add_extra_contracts(position['transactions'][int(spread_end):],risk_unit,portfolio_cash,config)
#         print("ERROR in build_trade, no trades")
#         print(position['position_id'])
#         return [], []
#         # if sized_buys == None:
#         # else:
#         #     return [sized_buys], [sized_sells]
    
#     return sized_buys, sized_sells

# def bet_sizer(contract_costs,buy_orders,sell_orders,risk_unit,portfolio_cash,config,open_datetime):
#     ## FUNDS ADJUSTMENT
#     available_funds = portfolio_cash
#     ## PUT ADJUSTMENT
#     target_cost = (risk_unit * available_funds)


#     quantities = size_spread_quantities(contract_costs, target_cost, config,open_datetime)
#     # quantities = finalize_trade(buy_orders, spread_cost, target_cost)
#     buy_df = pd.DataFrame.from_dict(buy_orders)
#     sell_df = pd.DataFrame.from_dict(sell_orders)
#     buy_df['quantity'] = 0
#     sell_df['quantity'] = 0


#     if len(quantities) == 0:
#         return None, None
#     for index, row in quantities.iterrows():
#         try:
#             if row['quantity'] == 0:
#                 continue
#             else:
#                 try:
#                     buy_df.loc[buy_df['option_symbol'] == row['option_symbol'], 'quantity'] = row['quantity']
#                     sell_df.loc[sell_df['option_symbol'] == row['option_symbol'], 'quantity'] = row['quantity']
#                 except Exception as e:
#                     print(f"Error {e} in size_trade {row}")
#                     print(e)
#                     print(buy_df)
#                     print(sell_df)
#                     print()
#                     return [], []
#         except Exception as e:
#             print("size_trade 2")
#             print(e)
#             print(buy_orders)
#             return [], []

#     buy_df = buy_df.loc[buy_df['quantity'] > 0]
#     sell_df = sell_df.loc[sell_df['quantity'] > 0]
#     buy_orders = buy_df.to_dict('records')
#     sell_orders = sell_df.to_dict('records')
#     return buy_orders, sell_orders

# def add_extra_contracts(positions, risk_unit,portfolio_cash,config):
#     target_cost = (risk_unit * portfolio_cash)
#     for position in positions:
#         print(position)
#         if position['buy_info']['contract_cost'] <= target_cost:
#             position['buy_info']['quantity'] = 1
#             position['sell_info']['quantity'] = 1
#             position['sell_info']['close_trade_dt'] = position['close_trade_dt']
#             return position['buy_info'], position['sell_info']
#         else:
#             continue
#     return None, None

# def size_spread_quantities(contracts_details, target_cost, config, open_datetime):
#     dt = datetime.strptime(open_datetime, "%Y-%m-%d %H:%M")
#     day_of_week = dt.weekday()
#     spread_start, spread_end = config['spread_search'].split(":")
#     adjusted_target_cost = target_cost
#     spread_length = config['spread_length']

#     if spread_length == 4:
#         if day_of_week == 3:
#             adjusted_contracts = contracts_details[int(spread_start):int(spread_end)-2]
#             spread_length -= 2
#         elif day_of_week == 2:
#             adjusted_contracts = contracts_details[int(spread_start):int(spread_end)-1]
#             spread_length -= 1
#         else:
#             adjusted_contracts = contracts_details[int(spread_start):int(spread_end)]
#     else:
#         if day_of_week >= 2:
#             # print(f"Day of week is greater than 3 {dt}")
#             # print(f"{(int(spread_start)-1)}:{(int(spread_end)-1)}")
#             adjusted_contracts = contracts_details[(int(spread_start)-1):(int(spread_end)-1)]
#         else:
#             # print(f"Day of week is less than 3 {dt}")
#             # print(f"{(int(spread_start))}:{(int(spread_end))}")
#             adjusted_contracts = contracts_details[int(spread_start):int(spread_end)]

#     quantities = []
#     contract_quantity = 0
#     spread_candidates = configure_contracts_for_trade_pct_based(adjusted_contracts, adjusted_target_cost, spread_length)
#     print
#     print(spread_candidates)
#     # spread_candidates, spread_cost = configure_contracts_for_trade(adjusted_contracts, adjusted_target_cost, spread_length)
#     # total_cost = 0

#     if len(spread_candidates) == 0:
#         return []
#     # else:
#     #     while total_cost < adjusted_target_cost:
#     #         if (spread_cost + total_cost) < adjusted_target_cost:
#     #             total_cost += spread_cost
#     #             contract_quantity += 1
#     #         else:
#     #             break
            
#     # formatted_spread_cost = 0
#     # for candidate in spread_candidates:
#     #         quantities.append({"option_symbol": candidate['option_symbol'], "quantity": contract_quantity,"contract_cost": candidate['contract_cost']})
#     #         formatted_spread_cost += (candidate['contract_cost'] * contract_quantity)

#     # cost_remaining = adjusted_target_cost - formatted_spread_cost
#     # # adjusted_quantities = []
#     # if cost_remaining > 0:
#     #     for quantity in quantities:
#     #         if quantity["contract_cost"] < cost_remaining:
#     #             cost_remaining -= quantity['contract_cost']
#     #             quantity['quantity'] += 1


#     details_df = pd.DataFrame(spread_candidates)
#     details_df = details_df.loc[details_df['quantity'] > 0]
#     details_df.reset_index(drop=True, inplace=True)
#     details_df['spread_position'] = details_df.index
#     return details_df

# # def size_next_contract(contracts_details, target_cost):
#      #Size the trades iteratively so there is flexibility in the length of the spread
# def configure_contracts_for_trade(contracts_details, target_cost, spread_length):
#     spread_candidates = []
#     total_cost = 0
#     cost_remaining = target_cost
#     for contract in contracts_details:
#         if contract['contract_cost'] < cost_remaining:
#             spread_candidates.append(contract)
#             cost_remaining -= contract['contract_cost']
#             total_cost += contract['contract_cost']
#         if len(spread_candidates) == spread_length:
#             return spread_candidates, total_cost
#     return spread_candidates, total_cost

# def configure_contracts_for_trade_pct_based(contracts_details, target_cost, spread_length):
#     spread_candidates = []
#     cost_remaining = target_cost
#     split_cost = target_cost / spread_length
#     for contract in contracts_details:
#         contract_quantity = 0
#         split_cost_remaining = split_cost
#         while split_cost_remaining > 0:
#             if (split_cost_remaining - contract['contract_cost']) > 0:
#                 split_cost_remaining -= contract['contract_cost']
#                 contract_quantity += 1
#             else:
#                 break

#         spread_candidates.append({"option_symbol": contract['option_symbol'], "quantity": contract_quantity,"contract_cost": contract['contract_cost']})
#     return spread_candidates
    

# def calculate_spread_cost(contracts_details):
#     cost = 0
#     for contract in contracts_details:
#         cost += contract['contract_cost']
#     return cost

# def build_volume_features(df):
#     avg_volume = df['v'].mean()
#     avg_transactions = df['n'].mean()
#     return avg_volume, avg_transactions

# def finalize_trade(contracts_details, spread_cost, target_cost):
#     if len(contracts_details) == 1:
#         spread_multiplier = math.floor(target_cost/spread_cost)
#         return [spread_multiplier]
#     elif len(contracts_details) == 2:
#         if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
#             return [1,1]
#         elif spread_cost > (1.1*target_cost):
#             spread2_cost = calculate_spread_cost(contracts_details[1:])
#             if spread2_cost < (1.1*target_cost):
#                 return [0,1]
#             else:
#                 contract = contracts_details[0]
#                 single_contract_cost = 100 * contract['contract_cost']
#                 if single_contract_cost > (1.1*target_cost):
#                     contract = contracts_details[1]
#                     single_contract_cost = 100 * contract['contract_cost']
#                     if single_contract_cost > (1.1*target_cost):
#                         return [0,0]
#                 else:
#                     return [1,0]
#         elif spread_cost < (.9*target_cost):
#             spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
#             if add_one:  
#                 return [(spread_multiplier+1),spread_multiplier]
#             else:
#                 return [spread_multiplier,spread_multiplier]
#         else:
#             print("ERROR")
#             return [0,0]
#     elif len(contracts_details) >= 3:
#         contracts_details = contracts_details[0:3]
#         if (1.1*target_cost) >= spread_cost >= (.9*target_cost):
#             return [1,1,1]
#         elif spread_cost > (1.1*target_cost):
#             spread2_cost = calculate_spread_cost(contracts_details[1:])
#             if spread2_cost < (1.1*target_cost):
#                 return [0,1,1]
#             else:
#                 contract = contracts_details[0]
#                 single_contract_cost = 100 * contract['contract_cost']
#                 if single_contract_cost > (1.1*target_cost):
#                     contract = contracts_details[1]
#                     single_contract_cost = 100 * contract['contract_cost']
#                     if single_contract_cost > (1.1*target_cost):
#                         contract = contracts_details[2]
#                         single_contract_cost = 100 * contract['contract_cost']
#                         if single_contract_cost > (1.1*target_cost):
#                                 return [0,0,0]
#                         else:
#                             return [0,0,1]
#                     else:
#                         return [0,1,0]
#                 else:
#                     return [1,0,0] 
#         elif spread_cost < (.9*target_cost):
#             spread_multiplier, add_one = add_spread_cost(spread_cost, target_cost, contracts_details)
#             if add_one:  
#                 return [(spread_multiplier+1),spread_multiplier,spread_multiplier]
#             else:
#                 return [spread_multiplier,spread_multiplier,spread_multiplier]
#         else:
#             print("ERROR")
#             return [0,0,0]
#     else:
#         return "NO TRADES"
            
# def add_spread_cost(spread_cost, target_cost, contracts_details):
#     add_one = False
#     spread_multiplier = 1
#     total_cost = spread_cost
#     if spread_cost == 0:
#         return 0, False
#     else:
#         while total_cost <= (1.1*target_cost):
#             spread_multiplier += 1
#             total_cost = spread_cost * spread_multiplier
        
#         if total_cost > (1.1*target_cost):
#             spread_multiplier -= 1
#             total_cost -= spread_cost

#         if total_cost < (.67*target_cost):
#             add_one = True

#     return spread_multiplier, add_one
        

# def build_trade_analytics(row, polygon_df, derivative_open_price, index, quantity, sell_code):
#     trade_dict = {}
#     before_df = polygon_df.iloc[:index]
#     after_df = polygon_df.iloc[index:]
#     trade_dict['max_value_before'] = before_df['h'].max()
#     trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
#     trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date'].strftime("%Y-%m-%d %H:%M")
#     trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)

#     if len(after_df) > 0:
#         trade_dict['max_value_after'] = after_df['h'].max()
#         trade_dict['max_value_after_idx'] = after_df['h'].idxmax()
#         trade_dict['max_value_after_date'] = after_df.loc[trade_dict['max_value_after_idx']]['date'].strftime("%Y-%m-%d %H:%M")
#         trade_dict['max_value_after_pct_change'] = ((trade_dict['max_value_after'] - derivative_open_price)/derivative_open_price)
#     else:
#         trade_dict['max_value_after'] = None
#         trade_dict['max_value_after_idx'] = None
#         trade_dict['max_value_after_date'] = None
#         trade_dict['max_value_after_pct_change'] = None

#     trade_dict["close_price"] = row['o']
#     trade_dict["close_datetime"] = row['date'].to_pydatetime()
#     trade_dict["quantity"] = quantity
#     trade_dict["contract_cost"] = (row['o']*100)
#     trade_dict["option_symbol"] = row['ticker']
#     trade_dict["sell_code"] = sell_code
#     return trade_dict


# # def build_trade_analytics_RMF(row, polygon_df, derivative_open_price, index, quantity, sell_code, aggregate_classification, hilo_score):
# #     trade_dict = {}
# #     before_df = polygon_df.iloc[:index]
# #     after_df = polygon_df.iloc[index:]
# #     trade_dict['max_value_before'] = before_df['h'].max()
# #     trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
# #     trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date'].strftime("%Y-%m-%d %H:%M")
# #     trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)

# #     if len(after_df) > 0:
# #         trade_dict['max_value_after'] = after_df['h'].max()
# #         trade_dict['max_value_after_idx'] = after_df['h'].idxmax()
# #         trade_dict['max_value_after_date'] = after_df.loc[trade_dict['max_value_after_idx']]['date'].strftime("%Y-%m-%d %H:%M")
# #         trade_dict['max_value_after_pct_change'] = ((trade_dict['max_value_after'] - derivative_open_price)/derivative_open_price)
# #     else:
# #         trade_dict['max_value_after'] = None
# #         trade_dict['max_value_after_idx'] = None
# #         trade_dict['max_value_after_date'] = None
# #         trade_dict['max_value_after_pct_change'] = None

# #     trade_dict["close_price"] = row['alert_price']
# #     trade_dict["close_datetime"] = row['date'].to_pydatetime()
# #     trade_dict["quantity"] = quantity
# #     trade_dict["contract_cost"] = (row['alert_price']*100)
# #     trade_dict["option_symbol"] = row['ticker']
# #     trade_dict["sell_code"] = sell_code
# #     trade_dict["aggregate_classification"] = aggregate_classification
# #     trade_dict["hilo_score"] = hilo_score
# #     return trade_dict