import math


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
    spread_cost = sum(contract_costs[0:3])
    quantities = finalize_trade(buy_orders, spread_cost, target_cost)

    if quantities == [0,0,0]:
        if len(contract_costs) > 3:
            spread_cost = contract_costs[3] * 100
            if spread_cost < target_cost:
                quantities = [0,0,0,1]

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
        

def build_trade_analytics(row, polygon_df, derivative_open_price, index, quantity, sell_code):
    trade_dict = {}
    before_df = polygon_df.iloc[:index]
    after_df = polygon_df.iloc[index:]
    trade_dict['max_value_before'] = before_df['h'].max()
    trade_dict['max_value_before_idx'] = before_df['h'].idxmax()
    trade_dict['max_value_before_date'] = before_df.loc[trade_dict['max_value_before_idx']]['date'].strftime("%Y-%m-%d %H:%M")
    trade_dict['max_value_before_pct_change'] = ((trade_dict['max_value_before'] - derivative_open_price)/derivative_open_price)

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