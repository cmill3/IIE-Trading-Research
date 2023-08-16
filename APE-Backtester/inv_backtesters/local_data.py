import boto3
import helpers.backtest_functions as back_tester
import pandas as pd



def s3_to_local(file_name):
    data1, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice_top30FLowtuned", file_name = f"{file_name}.csv",prefixes=["gainers","vdiff_gainC"])
    data2, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_priceShortDH_top30FLtuned", file_name = f"{file_name}.csv",prefixes=["losers"])
    data3, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice3_top30FLLowtuned", file_name = f"{file_name}.csv",prefixes=["ma","maP"])
    data4, _ = back_tester.pull_data_invalerts(bucket_name="icarus-research-data", object_key="backtesting_data/inv_alerts/1D_fiveDFeaturesPrice2_top30FLowtuned", file_name = f"{file_name}.csv",prefixes=["vdiff_gainP"])
    data = pd.concat([data1,data2,data3,data4],ignore_index=True)
    data.to_csv(f'/Users/charlesmiller/Documents/backtesting_data/1D/{file_name}.csv', index=False)

if __name__ == "__main__":
    file_names = ["2023-01-02","2023-01-09","2023-01-16","2023-01-23","2023-01-30","2023-02-06",
                  "2023-02-13","2023-02-20","2023-02-27","2023-03-06"
                  ,"2023-03-13","2023-03-20","2023-03-27","2023-04-03","2023-04-10","2023-04-17",
                  "2023-04-24","2023-05-01","2023-05-08","2023-05-15","2023-05-22","2023-05-29","2023-06-05","2023-06-12.csv"
                  ,"2023-06-19.csv","2023-06-26.csv"]
    for file_name in file_names:
        s3_to_local(file_name)