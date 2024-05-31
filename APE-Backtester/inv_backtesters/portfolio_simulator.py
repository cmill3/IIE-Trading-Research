import pandas as pd
import boto3

starting_capital = 60000

# cdvol_strategy_24 = {
#     "name": "cm3/20240514-24:CDVOLBF3-55PE_CD.33,.33,.33_vol0.4_rc10000_ds_vc100+200+300+300_sssl1:4:3$20000_0.03",
#     "results": [2.60,.18,0.40,.23]
# }

# cdvol_strategy_23 = {
#     "name": "cm3/20240514-23:CDVOLBF3-55PE_CD.33,.33,.33_vol0.4_rc10000_ds_vc100+200+300+300_sssl1:4:3$20000_0.03",
#     "results": [1.11,-0.41,2.53,-.10,4.22,0.13,1.43,1.54,-.24,-.61,.04,0.13]
# }

# cdvol_strategy_22 = {
#     "name": "cm3/20240516-22:CDVOLBF3-55PE_CD.33,.33,.33_div3_rc10000_vol0.4_DS_vc100+200+300+300_sssl1:4:3$20000_0.035",
#     "results": [1.84,.98,.14,.69,.2,0.0,1.96,-0.02,-.23,.79,0.59,-0.32]
# }
cdvol_strategy_24 = {
    "name": "cm3/20240520-24:reupv2_CDVOLBF3-55PE2_CDVOLVARVC_CD.33,.33,.33_div3_rc10000_vol0.4_DS_vc100+200+300+300_sssl1:4:3",
    "results": [1.47,-.36,0.02,-0.44]
}

# cdvol_strategy_23 = {
#     "name": "cm3/20240517-22:CDVOLBF3-6PE2_CD.33,.33,.33_div3_rc10000_vol0.4_DS_vc100+250+400+300_sssl1:4:3",
#     "results": [2.61,-0.48,1.99,-.10,7.08,-0.15,2.23,1.15,-.31,0.0,-.11,0.85]
# }

cdvol_strategy_23 = {
    "name": "cm3/20240520-23:reupv2_CDVOLBF3-55PE2_CDVOLVARVC_CD.33,.33,.33_div3_rc10000_vol0.4_DS_vc100+200+300+300_sssl1:4:3",
    "results": [.51,-.58,1.53,-.33,2.18,-0.08,1.33,1.92,-.52,-.82,.31,-.01]
}

cdvol_strategy_22 = {
    "name": "cm3/20240520-22:reupv2_CDVOLBF3-55PE2_CDVOLVARVC_CD.33,.33,.33_div3_rc10000_vol0.4_DS_vc100+200+300+300_sssl1:4:3",
    "results": [1.89,.16,-.04,0.74,1.41,.08,0.82,-.86,-.22,.51,0.31,.15]
}

# cdvol_strategy_22 = {
#     "name": "cm3/20240517-22:CDVOLBF3-6PE2_CD.33,.33,.33_div3_rc10000_vol0.4_DS_vc100+250+400+300_sssl1:4:3",
#     "results": [14.78,2.18,0.0,.19,.22,2.0,.0,-0.61,-.38,.44,0.92,-0.19]
# }

# portfolio_config_bad = {
#     "capital": starting_capital,
#     "units_traded": 4,
#     "results" : []
# }

year_results = [
    # cdvol_strategy_22, 
    cdvol_strategy_23,
    # cdvol_strategy_24
    ]
def determine_capital_allocation(starting_capital, capital, risk_units):
    if capital <= starting_capital:
        capital_allocated = capital / risk_units
    elif capital > starting_capital:
        capital_allocated = (starting_capital / risk_units) + ((capital - starting_capital) * .5)
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
        gain = (total_capital - capital) / capital
        trading_gain = (cdvol_ending_capital - strategy_allotment) / strategy_allotment

        portfolio_config["results"].append({
            "PERIOD": f"MONTH {i+1}",
            "TOTAL GAIN": round(gain,2),
            # "TREND GAIN": (trend_ending_capital - strategy_allotment) / strategy_allotment,
            "CDVOL GAIN":round(trading_gain,2),
            "STARTING CAPITAL": round(capital,2),
            "ENDING CAPITAL": round(total_capital,2),
            "CAPITAL ALLOCATED": round(capital_allocated,2),
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
        print(f'AVG MONTHLY GAIN:{round(sum(cdvol_strategy["results"])/len(cdvol_strategy["results"]),2)}')
        print()
        print(f'PORTFOLIO SIMULATOR RESULTS: {round((portfolio_config["capital"] - starting_capital)/starting_capital,2)}')
        print(f'FINAL CAPITAL: ${round(portfolio_config["capital"],2)}')
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
        




