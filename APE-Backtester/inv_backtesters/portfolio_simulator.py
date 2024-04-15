import pandas as pd
import boto3

starting_capital = 300000

# trend_strategy = {
#     "name": "cm3-20240201-modelTRENDNOLIM_dwnsdVOL:simple_TL15RM_vol1_vc400_dynamicscale$100000_0.0017",
#     "results": [1.51,-.07,.55,-.28,.43,-.08,1.8,-.34,.59,1.37,.57,1.02]
# }

# cdvol_strategy = {
#     "name": "cm3-20240201-modelCDVOLNOLIM_dwnsdVOL:CDVOL_CDVOLBF2_vol1_vc400_dynamicscale$100000_0.0019",
#     "results": [.51,.28,1.81,-.35,4.69,-.2,1.12,-.02,.49,.48,.02,.43]
# }

# trend_strategy = {
#     "name": "cm3-20240207-modelTRENDNOLIM_dwnsdVOL:stdclsAGG_TL15RMHT_vol1_vc600_dynamicscale_sa1$10000_0.005",
#     "results": [4.78,.18,.67,-.34,.21,.32,1.45,-.42,1.43,3.05,.44,1.56]
# }

cdvol_strategy_21 = {
    "name": "cm3-20240325-21-modelCDVOL_dwnsdVOL_BT2:CDVOLVARVC_CDVOLBF3-6_vol0.4_vc100/300/500_dynamicscale_sssl1:3:2$100000_0.0055",
    "results": [.72,.1,2.04,-.1,1.24,.13,1.73,.17,-.07,.56,1.79,-.24]
}

cdvol_strategy_22 = {
    "name": "cm3-20240325-22-modelCDVOL_dwnsdVOL_BT2:CDVOLVARVC_CDVOLBF3-6_vol0.4_vc100/300/500_dynamicscale_sssl1:3:2$100000_0.0055",
    "results": [6.35,5.64,.01,.85,3.57,.31,1.55,1.02,1.62,-.22,1.28,.17]
}

cdvol_strategy_23 = {
    "name": "cm3-20240325-23-modelCDVOL_dwnsdVOL_BT2:CDVOLVARVC_CDVOLBF3-6_vol0.4_vc100/300/500_dynamicscale_sssl1:3:2$100000_0.0055",
    "results": [1.22,.8,2.06,-.02,3.93,.38,1.91,.41,2.11,.89,.1,.01]
}

# portfolio_config_bad = {
#     "capital": starting_capital,
#     "units_traded": 4,
#     "results" : []
# }

year_results = [cdvol_strategy_21, cdvol_strategy_22, cdvol_strategy_23]
def determine_capital_allocation(starting_capital, capital, risk_units):
    if capital <= starting_capital:
        capital_allocated = capital / risk_units
    elif capital > starting_capital:
        capital_allocated = (starting_capital / risk_units) + ((capital - starting_capital) * .8)
    else:
        print(capital)
        raise ValueError("Capital is not correctly allocated. Please check the capital allocation.")
    return capital_allocated

def run_portfolio_simulator(cdvol_strategy):
    portfolio_config = {
    "capital": starting_capital,
    "units_traded": 3,
    "results" : []
    }
    for i in range(0,12):
        capital = portfolio_config["capital"]
        risk_units = portfolio_config["units_traded"]
        cdvol_return = cdvol_strategy["results"][i]

        capital_allocated = determine_capital_allocation(starting_capital, capital, risk_units)

        ## TWO BC of CDVOL and TREND. This would change as we add strategies
        strategy_allotment = capital_allocated 

        cdvol_ending_capital = (cdvol_return * strategy_allotment) + strategy_allotment

        total_capital = cdvol_ending_capital + (capital - capital_allocated)

        portfolio_config["results"].append({
            "PERIOD": f"MONTH {i+1}",
            "TOTAL GAIN": (total_capital - capital) / capital,
            # "TREND GAIN": (trend_ending_capital - strategy_allotment) / strategy_allotment,
            "CDVOL GAIN": (cdvol_ending_capital - strategy_allotment) / strategy_allotment,
            "TRADING GAIN": ((cdvol_ending_capital) - capital_allocated)/capital_allocated,
            "CASH": total_capital
        })

        portfolio_config["capital"] = total_capital

    return portfolio_config

# def run_portfolio_simulator_bad_outcomes():
#     for i in range(0,12):
#         capital = portfolio_config_bad["capital"]
#         risk_units = portfolio_config_bad["units_traded"]
#         cdvol_return = cdvol_strategy["results"][i]

#         if cdvol_return < 0:
#             cdvol_return *= 1.3
#         else:
#             cdvol_return *= .7

#         trend_return = trend_strategy["results"][i]

#         if trend_return < 0:
#             trend_return *= 1.3
#         else:
#             trend_return *= .7

#         capital_allocated = determine_capital_allocation(starting_capital, capital, risk_units)

#         ## TWO BC of CDVOL and TREND. This would change as we add strategies
#         strategy_allotment = capital_allocated / 2

#         trend_ending_capital = (trend_return * strategy_allotment) + strategy_allotment
#         cdvol_ending_capital = (cdvol_return * strategy_allotment) + strategy_allotment

#         total_capital = trend_ending_capital + cdvol_ending_capital + (capital - capital_allocated)

#         portfolio_config_bad["results"].append({
#             "PERIOD": f"MONTH {i+1}",
#             "TOTAL GAIN": (total_capital - capital) / capital,
#             "TREND GAIN": (trend_ending_capital - strategy_allotment) / strategy_allotment,
#             "CDVOL GAIN": (cdvol_ending_capital - strategy_allotment) / strategy_allotment,
#             "TRADING GAIN": ((trend_ending_capital + cdvol_ending_capital) - capital_allocated)/capital_allocated
#         })

#         portfolio_config_bad["capital"] = total_capital

#     return portfolio_config_bad


if __name__ == "__main__":
    # df = pd.DataFrame(portfolio_config["results"])
    # df.to_csv("portfolio_results.csv", index=False)
    # s3 = boto3.client("s3")
    # s3.upload_file("portfolio_results.csv", "investing-strategy", "portfolio_results.csv")
    for cdvol_strategy in year_results:
        portfolio_config = run_portfolio_simulator(cdvol_strategy)
        # print(f'TREND STRATEGY: {trend_strategy["name"]}')
        # print(f'AVG MONTHLY GAIN:{sum(trend_strategy["results"])/len(trend_strategy["results"])}')
        # print()
        print(f'CDVOL STRATEGY: {cdvol_strategy["name"]}')
        print(f'AVG MONTHLY GAIN:{sum(cdvol_strategy["results"])/len(cdvol_strategy["results"])}')
        print()
        print(f'PORTFOLIO SIMULATOR RESULTS: {(portfolio_config["capital"] - starting_capital)/starting_capital}')
        print(f'FINAL CAPITAL: ${portfolio_config["capital"]}')
        print()
        for result in portfolio_config["results"]:
            print(result)
            print()
    
    # print()
    # print("BAD OUTCOMES")
    # print()
    # portfolio_config_bad = run_portfolio_simulator_bad_outcomes()
    # print(f'PORTFOLIO SIMULATOR RESULTS: {(portfolio_config_bad["capital"] - starting_capital)/starting_capital}')
    # print(f'FINAL CAPITAL: ${portfolio_config_bad["capital"]}')
    # print()
    # for result in portfolio_config["results"]:
    #     print(result)
    #     print()
        




