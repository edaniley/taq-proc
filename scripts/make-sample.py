import os, sys
import numpy as np
import pandas as pd
import datetime

input_fields = ['ID', 'Symbol', 'Date', 'StartTime', 'EndTime', 'Side', 'OrdQty', 'LimitPx', 'MPA', 'ExecTime', 'ExecQty']

def PrepareSampleROD(input_file):
  output_file = ".".join(input_file.split(".")[:-1]) + ".hdf"
  sample = pd.read_csv(input_file)
  sample["ID"] = sample["mpid"] + "." + sample["order_id"]
  sample["Date"] = sample["entry_time"].apply(lambda x: str(x)[:10])
  sample["StartTime"] = sample["entry_time"].apply(lambda x: str(x).split("+")[0][11:])
  sample["EndTime"] = sample["end_time"].apply(lambda x: str(x).split("+")[0][11:])
  #sample["Side"] = sample["side"].apply(lambda x: "" if np.isnan(x) else str(x)[:1])
  sample["ExecTime"] = sample["time"].apply(lambda x: str(x).split("+")[0][11:])
  sample.rename(columns = { "symbol": "Symbol",
                            "side": "Side",
                            "astralcalc_venue_order_nonlimit_max_wpa" : "MPA",
                            "limit_price": "LimitPx",
                            "order_quantity": "OrdQty",
                            "lastsize": "ExecQty"
                          }, inplace = True)
  df = sample.loc[sample["astralcalc_is_resting_eligible"] == True, input_fields]
  df.to_hdf(output_file, "data", mode="w")

if __name__ == "__main__":
  input_file = "sample.csv"
  if len(sys.argv) > 1:
    input_file = sys.argv[1]
  PrepareSampleROD(input_file)
