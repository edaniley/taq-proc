import os, sys
import argparse
import numpy as np
import pandas as pd
import datetime
import json
import uuid
import taqpy

taqpy_addr = "127.0.0.1:21090"
function_def = json.loads(taqpy.Describe())

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
  input_fields = []
  for f in function_def["ROD"]["input_fields"]:
    input_fields.append(f[0])
  df = sample.loc[sample["astralcalc_is_resting_eligible"] == True, input_fields]
  df.to_hdf(output_file, "data", mode="w")
  return df

def MakeRequestJson(df, function_name, time_zone):
  if function_name not in taqpy.FunctionList():
    raise Exception("Unknown function")
  argument_list = taqpy.ArgumentNames(function_name)
  for argument_name in argument_list:
    if argument_name not in df.columns:
      raise Exception("Function:{} missing argument:{}".format(function_name, argument_name))

  hdr = {}
  hdr["tcp"] = taqpy_addr
  hdr["request_id"] = str(uuid.uuid1())
  hdr["function_list"] = [function_name]
  hdr["argument_list"] = argument_list
  hdr["separator"] = "|"
  hdr["input_sorted"] = False
  hdr["input_cnt"] = len(df)
  hdr["output_format"] = "psv"
  hdr["time_zone"] = time_zone
  return json.dumps(hdr)

def ExecuteQuery(df, function_name, time_zone):
  hdr_str = MakeRequestJson(df, function_name, time_zone)
  kwargs = {}
  for field in taqpy.ArgumentList(function_name):
    kwargs[field[0]] = np.array(df[field[0]], dtype=field[1])

  ret = taqpy.Execute(hdr_str, **kwargs)
  ret_json = json.loads(ret[0])
  if "error_summary" not in ret_json.keys() or type(ret_json["error_summary"]) != type([]):
    ret_json["error_summary"] = []

  ret_data = None
  if len(ret) > 1:
    data = {}
    col = 1
    for field in taqpy.ResultFields(function_name):
      data[field[0]] = pd.Series(ret[col])
      col += 1
    ret_data = pd.DataFrame(data)

  return ret_json, ret_data

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-f", "--function", type=str, default="", help="function name [Quote ROD]")
  parser.add_argument("-i", "--in-file", type=str, default="", help="input file")
  parser.add_argument("-d", "--data-format", type=str, default="hdf", help="input file format [hdf, csv, psv]" )
  parser.add_argument("-t", "--time-zone", type=str, default="UTC", help="Timezone name [UTC, America/New_York]" )
  parser.add_argument("-o", "--out-file", type=str, default="", help="output file")
  try:
    args = parser.parse_args()
    if args.function not in function_def.keys():
      raise Exception("Unsupported function:'{}'".format(args.function))
    if args.data_format not in ["hdf", "hdf5", "csv", "psv"]:
      raise Exception("Unsupported date format:'{}'".format(args.data_format))

    start_time = datetime.datetime.now()
    df = pd.read_hdf(args.in_file)
    read_time = datetime.datetime.now()
    ret_json, ret_data = ExecuteQuery(df, args.function, args.time_zone)
    exec_time = datetime.datetime.now()
    write_time = exec_time
    if len(ret_json["error_summary"]) > 0:
      print("Query resulted in errors :", file=sys.stderr)
      for err in ret_json["error_summary"]:
        print("ErrorType {} : {}".format(err["type"], err["count"]), file=sys.stderr)
    if int(ret_json["output_records"]) > 0 and args.out_file:
      ret_data.to_hdf(args.out_file, "data", mode="w")
      write_time = datetime.datetime.now()
    end_time = datetime.datetime.now()

    print("started:{} read-time:{} exec-time:{} write-time:{} run-time:{}".format(
      start_time, read_time-start_time, exec_time-read_time, write_time-exec_time, end_time - start_time))

  except Exception as ex:
    print(ex)
    