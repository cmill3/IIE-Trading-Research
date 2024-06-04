from datetime import datetime, timedelta
from helpers.backtrader_helper import *
from helpers.strategy_helper import *
from helpers.backtest_functions import *    


def simulate_portfolio_daily_rebalance(positions_list, datetime_list, portfolio_cash, risk_unit,put_adjustment,config,results_dict_func):
    positions_taken = []
    contracts_bought = []
    contracts_sold = []
    starting_cash = portfolio_cash
    starting_reserve = config['reserve_cash']
    current_reserve = starting_reserve
    sales_dict = {}
    portfolio_dict, positions_dict, passed_trades_dict = convert_lists_to_dicts_inv(positions_list, datetime_list)

    ## What we need is to at this point build the trade. We need to send through the package of contracts in 
    ## their bundle of a "position", then we can approximate bet sizing and the contract sizing at this point in time.
    ## Then from there we can build the results of the trade, and then we can build the portfolio from there.
    ## This will give us dynamic and rective sizing.
    
    i = 0
    current_day = None
    daily_cash = config['portfolio_cash'] * config['divisor']
    for key, value in portfolio_dict.items():
        current_positions = []
        if i == 0:
            current_day = key.day
            value['portfolio_cash'] = portfolio_cash
            value['open_positions_start'].extend(current_positions)

            if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if daily_cash > 0:
                        simulated_position = build_trade(position,risk_unit,put_adjustment,daily_cash,config)
                        if len(simulated_position) == 0:
                            continue
                        orders_taken = False
                        # quantities = {}
                        for transaction in simulated_position['transactions']:
                            sell_order = transaction['sell_info']
                            buy_order = transaction['buy_info']
                            print(f"Buy Order: {buy_order}")
                            print(f"Sell Order: {sell_order}")
                            if buy_order != None:
                                orders_taken = True
                                value['contracts_purchased'].append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                value['purchase_costs'] += (buy_order['contract_cost'] * buy_order['quantity'])
                                value['portfolio_cash'] -= (buy_order['contract_cost'] * buy_order['quantity'])
                                daily_cash -= (buy_order['contract_cost'] * buy_order['quantity'])
                                contracts_bought.append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                # quantities[order['option_symbol']] = order['quantity']
                                ### CHASE
                                # dt_str = sized_sells[index]['close_datetime'].strftime("%Y-%m-%d-%H-%M")
                                # year, month, day, hour, minute = dt_str.split("-")
                                # dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                                sale_dt = datetime.strptime(sell_order['close_trade_dt'], "%Y-%m-%d %H:%M")
                                if sales_dict.get(sale_dt) is None:
                                    sales_dict[sale_dt] = [sell_order]
                                else:
                                    sales_dict[sale_dt].append(sell_order)
                        if orders_taken:
                            current_positions.append((simulated_position['position_id'].split("-")[0] + simulated_position['position_id'].split("-")[1]))
                            results_dicts = extract_results_dict(simulated_position)
                            positions_taken.append({'position_id':simulated_position['position_id'],"results":results_dicts})
                            value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
                else:
                        if passed_trades_dict.get(key) is not None:
                            passed_trades_dict[key]['trades'].append(simulated_position)
                        else:
                            passed_trades_dict[key] = {
                                "trades": [simulated_position]
                            }
                i += 1
                value['open_positions_end'].extend(current_positions)
                positions_end = current_positions
                continue
        else:
            positions_open = positions_end
            positions_end = []
            value['portfolio_cash'] = portfolio_dict[key - timedelta(minutes=15)]['portfolio_cash']
            current_positions = positions_open
            value['open_positions_start'].extend(current_positions)
            check_reup = check_for_reup(key)
            if check_reup:
                value['portfolio_cash'],current_reserve = reup_cash(value['portfolio_cash'],current_reserve,starting_reserve,starting_cash)

            if key.day != current_day:
                print("KEY")
                print(key)
                current_day = key.day
                daily_cash = value['portfolio_cash']*(config['divisor'])
                print(f"Total Cash: {value['portfolio_cash']}")
                print(f"Adjusted Daily Cash: {daily_cash}")



        
        if sales_dict.get(key) is not None:
            # print(positions_dict)
            for sale in sales_dict.get(key):
                ### CHASE
                # opt_sym = sale['option_symbol'].split("O:")[1]
                if (f"{sale['option_symbol']}_{sale['order_id']}") in contracts_bought:
                    value['contracts_sold'].append(f"{sale['option_symbol']}_{sale['order_id']}")
                    value['sale_returns'] += (sale['contract_cost'] * sale['quantity'])
                    value['portfolio_cash'] += (sale['contract_cost'] * sale['quantity'])
                    if config['reup'] == 'hot':
                        daily_cash += (sale['contract_cost'] * sale['quantity'])
                    contracts_sold.append(f"{sale['option_symbol']}_{sale['order_id']}")
                    if (sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]) in current_positions:
                        current_positions.remove((sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]))    

        if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if daily_cash > 0:
                        simulated_position = build_trade(position,risk_unit,put_adjustment,daily_cash,config)
                        if len(simulated_position) == 0:
                            continue
                        orders_taken = False
                        for transaction in simulated_position['transactions']:
                            sell_order = transaction['sell_info']
                            buy_order = transaction['buy_info']
                            if buy_order != None:
                                orders_taken = True
                                value['contracts_purchased'].append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                value['purchase_costs'] += (buy_order['contract_cost'] * buy_order['quantity'])
                                value['portfolio_cash'] -= (buy_order['contract_cost'] * buy_order['quantity'])
                                daily_cash -= (buy_order['contract_cost'] * buy_order['quantity'])
                                contracts_bought.append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                # quantities[order['option_symbol']] = order['quantity']
                                ### CHASE
                                # dt_str = sized_sells[index]['close_datetime'].strftime("%Y-%m-%d-%H-%M")
                                # year, month, day, hour, minute = dt_str.split("-")
                                # dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                                sale_dt = datetime.strptime(sell_order['close_trade_dt'], "%Y-%m-%d %H:%M")
                                if sales_dict.get(sale_dt) is None:
                                    sales_dict[sale_dt] = [sell_order]
                                else:
                                    sales_dict[sale_dt].append(sell_order)
                        if orders_taken:
                            current_positions.append((simulated_position['position_id'].split("-")[0] + simulated_position['position_id'].split("-")[1]))
                            results_dicts = extract_results_dict(simulated_position)
                            positions_taken.append({'position_id':simulated_position['position_id'],"results":results_dicts})
                else:
                    if passed_trades_dict.get(key) is not None:
                        passed_trades_dict[key]['trades'].append(simulated_position)
                    else:
                        passed_trades_dict[key] = {
                            "trades": [simulated_position]
                        }
        
        value['open_positions_end'].extend(current_positions)
        positions_end = current_positions
        value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
        print()
        print(f"Daily Cash: {daily_cash} Portfolio Cash: {value['portfolio_cash']} of {key}")
        print()

    portfolio_df = pd.DataFrame.from_dict(portfolio_dict, orient='index')
    portfolio_df['reserve_cash'] = current_reserve
    passed_trades_df = pd.DataFrame.from_dict(passed_trades_dict, orient='index')
    print("Elements in bought but not in sold:")
    diff = list(set(contracts_bought) - set(contracts_sold))
    print(diff)
    print("Elements in sold but not in bought:")
    diff2 = list(set(contracts_sold) - set(contracts_bought))
    print(diff2)
    return portfolio_df, passed_trades_df, positions_taken, positions_dict

def simulate_portfolio_DS(positions_list, datetime_list, portfolio_cash, risk_unit,put_adjustment,config,results_dict_func):
    positions_taken = []
    contracts_bought = []
    contracts_sold = []
    starting_cash = portfolio_cash
    starting_reserve = config['reserve_cash']
    current_reserve = starting_reserve
    sales_dict = {}
    portfolio_dict, positions_dict, passed_trades_dict = convert_lists_to_dicts_inv(positions_list, datetime_list)

    ## What we need is to at this point build the trade. We need to send through the package of contracts in 
    ## their bundle of a "position", then we can approximate bet sizing and the contract sizing at this point in time.
    ## Then from there we can build the results of the trade, and then we can build the portfolio from there.
    ## This will give us dynamic and rective sizing.
    
    i = 0
    for key, value in portfolio_dict.items():
        current_positions = []
        if i == 0:
            value['portfolio_cash'] = portfolio_cash
            value['open_positions_start'].extend(current_positions)

            if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if value['portfolio_cash'] > (.05 * starting_cash):
                        trade_value = (value['portfolio_cash']*config['portfolio_pct'])/config['risk_unit']
                        print(f"Position: {position}")
                        print(f"Portfolio Cash: {value['portfolio_cash']}")
                        print(f"Trade Value: {trade_value}")
                        simulated_position = build_trade(position,risk_unit,put_adjustment,trade_value,config)
                        if len(simulated_position) == 0:
                            continue
                        orders_taken = False
                        # quantities = {}
                        for transaction in simulated_position['transactions']:
                            sell_order = transaction['sell_info']
                            buy_order = transaction['buy_info']
                            print(f"Buy Order: {buy_order}")
                            print(f"Sell Order: {sell_order}")
                            if buy_order != None:
                                orders_taken = True
                                value['contracts_purchased'].append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                value['purchase_costs'] += (buy_order['contract_cost'] * buy_order['quantity'])
                                value['portfolio_cash'] -= (buy_order['contract_cost'] * buy_order['quantity'])
                                contracts_bought.append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                # quantities[order['option_symbol']] = order['quantity']
                                ### CHASE
                                # dt_str = sized_sells[index]['close_datetime'].strftime("%Y-%m-%d-%H-%M")
                                # year, month, day, hour, minute = dt_str.split("-")
                                # dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                                sale_dt = datetime.strptime(sell_order['close_trade_dt'], "%Y-%m-%d %H:%M")
                                if sales_dict.get(sale_dt) is None:
                                    sales_dict[sale_dt] = [sell_order]
                                else:
                                    sales_dict[sale_dt].append(sell_order)
                        if orders_taken:
                            current_positions.append((simulated_position['position_id'].split("-")[0] + simulated_position['position_id'].split("-")[1]))
                            results_dicts = extract_results_dict(simulated_position)
                            positions_taken.append({'position_id':simulated_position['position_id'],"results":results_dicts})
                            value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
                else:
                        if passed_trades_dict.get(key) is not None:
                            passed_trades_dict[key]['trades'].append(simulated_position)
                        else:
                            passed_trades_dict[key] = {
                                "trades": [simulated_position]
                            }
                i += 1
                value['open_positions_end'].extend(current_positions)
                positions_end = current_positions
                continue
        else:
            positions_open = positions_end
            positions_end = []
            value['portfolio_cash'] = portfolio_dict[key - timedelta(minutes=15)]['portfolio_cash']
            current_positions = positions_open
            value['open_positions_start'].extend(current_positions)
            # check_reup = check_for_reup(key)
            # if check_reup:
            #     value['portfolio_cash'],current_reserve = reup_cash_v2(value['portfolio_cash'],current_reserve,starting_reserve,starting_cash)

        
        if sales_dict.get(key) is not None:
            # print(positions_dict)
            for sale in sales_dict.get(key):
                ### CHASE
                # opt_sym = sale['option_symbol'].split("O:")[1]
                if (f"{sale['option_symbol']}_{sale['order_id']}") in contracts_bought:
                    value['contracts_sold'].append(f"{sale['option_symbol']}_{sale['order_id']}")
                    value['sale_returns'] += (sale['contract_cost'] * sale['quantity'])
                    value['portfolio_cash'] += (sale['contract_cost'] * sale['quantity'])
                    contracts_sold.append(f"{sale['option_symbol']}_{sale['order_id']}")
                    if (sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]) in current_positions:
                        current_positions.remove((sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]))    

        if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if value['portfolio_cash'] > (.05 * starting_cash):
                        trade_value = (value['portfolio_cash']*config['portfolio_pct'])/config['risk_unit']
                        print(f"Position: {position}")
                        print(f"Portfolio Cash: {value['portfolio_cash']}")
                        print(f"Trade Value: {trade_value}")
                        simulated_position = build_trade(position,risk_unit,put_adjustment,trade_value,config)
                        if len(simulated_position) == 0:
                            continue
                        orders_taken = False
                        for transaction in simulated_position['transactions']:
                            sell_order = transaction['sell_info']
                            buy_order = transaction['buy_info']
                            if buy_order != None:
                                orders_taken = True
                                value['contracts_purchased'].append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                value['purchase_costs'] += (buy_order['contract_cost'] * buy_order['quantity'])
                                value['portfolio_cash'] -= (buy_order['contract_cost'] * buy_order['quantity'])
                                contracts_bought.append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                # quantities[order['option_symbol']] = order['quantity']
                                ### CHASE
                                # dt_str = sized_sells[index]['close_datetime'].strftime("%Y-%m-%d-%H-%M")
                                # year, month, day, hour, minute = dt_str.split("-")
                                # dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                                sale_dt = datetime.strptime(sell_order['close_trade_dt'], "%Y-%m-%d %H:%M")
                                if sales_dict.get(sale_dt) is None:
                                    sales_dict[sale_dt] = [sell_order]
                                else:
                                    sales_dict[sale_dt].append(sell_order)
                        if orders_taken:
                            current_positions.append((simulated_position['position_id'].split("-")[0] + simulated_position['position_id'].split("-")[1]))
                            results_dicts = extract_results_dict(simulated_position)
                            positions_taken.append({'position_id':simulated_position['position_id'],"results":results_dicts})
                else:
                    if passed_trades_dict.get(key) is not None:
                        passed_trades_dict[key]['trades'].append(simulated_position)
                    else:
                        passed_trades_dict[key] = {
                            "trades": [simulated_position]
                        }
        
        value['open_positions_end'].extend(current_positions)
        positions_end = current_positions
        value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])


    portfolio_df = pd.DataFrame.from_dict(portfolio_dict, orient='index')
    portfolio_df['reserve_cash'] = current_reserve
    passed_trades_df = pd.DataFrame.from_dict(passed_trades_dict, orient='index')
    print("Elements in bought but not in sold:")
    diff = list(set(contracts_bought) - set(contracts_sold))
    print(diff)
    print("Elements in sold but not in bought:")
    diff2 = list(set(contracts_sold) - set(contracts_bought))
    print(diff2)
    return portfolio_df, passed_trades_df, positions_taken, positions_dict


def simulate_portfolio_FIXED(positions_list, datetime_list, portfolio_cash, risk_unit,put_adjustment,config,results_dict_func):
    positions_taken = []
    contracts_bought = []
    contracts_sold = []
    starting_cash = portfolio_cash
    starting_reserve = config['reserve_cash']
    current_reserve = starting_reserve
    sales_dict = {}
    portfolio_dict, positions_dict, passed_trades_dict = convert_lists_to_dicts_inv(positions_list, datetime_list)

    ## What we need is to at this point build the trade. We need to send through the package of contracts in 
    ## their bundle of a "position", then we can approximate bet sizing and the contract sizing at this point in time.
    ## Then from there we can build the results of the trade, and then we can build the portfolio from there.
    ## This will give us dynamic and rective sizing.
    
    i = 0
    trading_capital = config['portfolio_cash'] * config['portfolio_pct']
    trade_value = trading_capital/config['risk_unit']
    print(f"Trade Value: {trade_value}")
    for key, value in portfolio_dict.items():
        current_positions = []
        if i == 0:
            value['portfolio_cash'] = portfolio_cash
            value['open_positions_start'].extend(current_positions)

            if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if value['portfolio_cash'] > (.05 * starting_cash):
                        simulated_position = build_trade(position,risk_unit,put_adjustment,trade_value,config)
                        if len(simulated_position) == 0:
                            continue
                        orders_taken = False
                        # quantities = {}
                        for transaction in simulated_position['transactions']:
                            sell_order = transaction['sell_info']
                            buy_order = transaction['buy_info']
                            print(f"Buy Order: {buy_order}")
                            print(f"Sell Order: {sell_order}")
                            if buy_order != None:
                                orders_taken = True
                                value['contracts_purchased'].append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                value['purchase_costs'] += (buy_order['contract_cost'] * buy_order['quantity'])
                                value['portfolio_cash'] -= (buy_order['contract_cost'] * buy_order['quantity'])
                                contracts_bought.append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                # quantities[order['option_symbol']] = order['quantity']
                                ### CHASE
                                # dt_str = sized_sells[index]['close_datetime'].strftime("%Y-%m-%d-%H-%M")
                                # year, month, day, hour, minute = dt_str.split("-")
                                # dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                                sale_dt = datetime.strptime(sell_order['close_trade_dt'], "%Y-%m-%d %H:%M")
                                if sales_dict.get(sale_dt) is None:
                                    sales_dict[sale_dt] = [sell_order]
                                else:
                                    sales_dict[sale_dt].append(sell_order)
                        if orders_taken:
                            current_positions.append((simulated_position['position_id'].split("-")[0] + simulated_position['position_id'].split("-")[1]))
                            results_dicts = extract_results_dict(simulated_position)
                            positions_taken.append({'position_id':simulated_position['position_id'],"results":results_dicts})
                            value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])
                else:
                        if passed_trades_dict.get(key) is not None:
                            passed_trades_dict[key]['trades'].append(simulated_position)
                        else:
                            passed_trades_dict[key] = {
                                "trades": [simulated_position]
                            }
                i += 1
                value['open_positions_end'].extend(current_positions)
                positions_end = current_positions
                continue
        else:
            positions_open = positions_end
            positions_end = []
            value['portfolio_cash'] = portfolio_dict[key - timedelta(minutes=15)]['portfolio_cash']
            current_positions = positions_open
            value['open_positions_start'].extend(current_positions)
            check_reup = check_for_reup(key,config)
            if check_reup:
                # elif config['reup'] == 'cold':
                #     if value['portfolio_cash'] > starting_cash:
                #         cash_surplus = value['portfolio_cash'] - starting_cash
                #         trade_value = (starting_cash+(.67*cash_surplus))/risk_unit
                #         print(f"New Trade Value Surplus: {trade_value}")
                #     else:
                #         trade_value = starting_cash/risk_unit
                #         print(f"New Trade Value: {trade_value}")        
                trading_capital = value['portfolio_cash'] * config['portfolio_pct']
                trade_value = trading_capital/config['risk_unit']
        
        if sales_dict.get(key) is not None:
            # print(positions_dict)
            for sale in sales_dict.get(key):
                ### CHASE
                # opt_sym = sale['option_symbol'].split("O:")[1]
                if (f"{sale['option_symbol']}_{sale['order_id']}") in contracts_bought:
                    value['contracts_sold'].append(f"{sale['option_symbol']}_{sale['order_id']}")
                    value['sale_returns'] += (sale['contract_cost'] * sale['quantity'])
                    value['portfolio_cash'] += (sale['contract_cost'] * sale['quantity'])
                    contracts_sold.append(f"{sale['option_symbol']}_{sale['order_id']}")
                    if (sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]) in current_positions:
                        current_positions.remove((sale['position_id'].split("-")[0] + sale['position_id'].split("-")[1]))    

        if positions_dict.get(key) is not None:
                for position in positions_dict[key]:
                    if value['portfolio_cash'] > (.05 * starting_cash):
                        print(f"Position: {position}")
                        print(f"Portfolio Cash: {value['portfolio_cash']}")
                        print()
                        simulated_position = build_trade(position,risk_unit,put_adjustment,trade_value,config)
                        if len(simulated_position) == 0:
                            continue
                        orders_taken = False
                        for transaction in simulated_position['transactions']:
                            sell_order = transaction['sell_info']
                            buy_order = transaction['buy_info']
                            if buy_order != None:
                                orders_taken = True
                                value['contracts_purchased'].append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                value['purchase_costs'] += (buy_order['contract_cost'] * buy_order['quantity'])
                                value['portfolio_cash'] -= (buy_order['contract_cost'] * buy_order['quantity'])
                                contracts_bought.append(f"{buy_order['option_symbol']}_{buy_order['order_id']}")
                                # quantities[order['option_symbol']] = order['quantity']
                                ### CHASE
                                # dt_str = sized_sells[index]['close_datetime'].strftime("%Y-%m-%d-%H-%M")
                                # year, month, day, hour, minute = dt_str.split("-")
                                # dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                                sale_dt = datetime.strptime(sell_order['close_trade_dt'], "%Y-%m-%d %H:%M")
                                if sales_dict.get(sale_dt) is None:
                                    sales_dict[sale_dt] = [sell_order]
                                else:
                                    sales_dict[sale_dt].append(sell_order)
                        if orders_taken:
                            current_positions.append((simulated_position['position_id'].split("-")[0] + simulated_position['position_id'].split("-")[1]))
                            results_dicts = extract_results_dict(simulated_position)
                            positions_taken.append({'position_id':simulated_position['position_id'],"results":results_dicts})
                else:
                    if passed_trades_dict.get(key) is not None:
                        passed_trades_dict[key]['trades'].append(simulated_position)
                    else:
                        passed_trades_dict[key] = {
                            "trades": [simulated_position]
                        }
        
        value['open_positions_end'].extend(current_positions)
        positions_end = current_positions
        value['period_net_returns'] = (value['sale_returns'] - value['purchase_costs'])


    portfolio_df = pd.DataFrame.from_dict(portfolio_dict, orient='index')
    portfolio_df['reserve_cash'] = current_reserve
    passed_trades_df = pd.DataFrame.from_dict(passed_trades_dict, orient='index')
    print("Elements in bought but not in sold:")
    diff = list(set(contracts_bought) - set(contracts_sold))
    print(diff)
    print("Elements in sold but not in bought:")
    diff2 = list(set(contracts_sold) - set(contracts_bought))
    print(diff2)
    return portfolio_df, passed_trades_df, positions_taken, positions_dict


def check_for_reup(dt,config):
    if config['reup'] == 'weekly':
        if dt.weekday() == 0:
            if dt.hour == 0: 
                if dt.minute == 0:
                    print("Reup Time Week")
                    return True
    elif config['reup'] == 'daily':
        if dt.hour == 0: 
            if dt.minute == 0:
                print("Reup Time Day")
                return True
    return False

def reup_cash(current_cash, reserve_cash, starting_reserve,starting_cash):
    print(f"Starting Cash: {starting_cash}")
    print(f"Current Cash: {current_cash}")
    if current_cash < starting_cash:
        debit_amt = starting_cash - current_cash

        if reserve_cash >= (starting_reserve/2):
            reup_amt = starting_reserve/2
        else:
            reup_amt = reserve_cash

        if debit_amt > reup_amt:
            print('debit_amt > reup_amt')
            current_cash += reup_amt
            reserve_cash -= reup_amt
            print(f"Reserve Cash: {reserve_cash}")
            print(f"Current Cash: {current_cash}")
            return current_cash, reserve_cash
        else:
            print('reupping but not depleting reserves')
            reserve_cash -= debit_amt
            current_cash += debit_amt
            print(f"Reserve Cash: {reserve_cash}")
            print(f"Current Cash: {current_cash}")
            return current_cash, reserve_cash
    elif current_cash > starting_cash:
        if reserve_cash == starting_reserve:
            print('reserve is full')
            print(f"Reserve Cash: {reserve_cash}")
            print(f"Current Cash: {current_cash}")
            return current_cash, reserve_cash
        else:
            cash_surplus = current_cash - starting_cash
            reserve_debit = starting_reserve - reserve_cash
            if cash_surplus > reserve_debit:
                print('reupping reserves full')
                current_cash -= reserve_debit
                reserve_cash = starting_reserve
                print(f"Reserve Cash: {reserve_cash}")
                print(f"Current Cash: {current_cash}")
                return current_cash, reserve_cash
            else:
                print('reupping reserves not full')
                reserve_cash += cash_surplus
                current_cash = starting_cash
                print(f"Reserve Cash: {reserve_cash}")
                print(f"Current Cash: {current_cash}")
                return current_cash, reserve_cash
    else:
        print('no change')
        print(f"Reserve Cash: {reserve_cash}")
        print(f"Current Cash: {current_cash}")
        return current_cash, reserve_cash
    

def reup_cash_v2(current_cash, reserve_cash, starting_reserve,starting_cash):
    reserve_to_add = starting_reserve/3
    if reserve_cash > 0:
        current_cash += reserve_to_add
        reserve_cash -= reserve_to_add
    return current_cash, reserve_cash