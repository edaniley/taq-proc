import sys
import datetime
import argparse
import random
import json
import uuid
import pandas as pd
import numpy as np
import taqpy
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

taqproc = "127.0.0.1:3091"

query_time = None

def Execute(args):
  if len(args.date) == 8:
    trade_date =  datetime.datetime(int(args.date[0:4]), int(args.date[4:6]), int(args.date[6:8]))
  elif len(args.date) == 10:
    trade_date =  datetime.datetime(int(args.date.split("-")[0]), int(args.date.split("-")[1]), int(args.date.split("-")[2]))
  else:
    raise Exception("Unknown Date format")

  df = pd.read_hdf(args.input_file)
  #df = df.head(10)
  df["Date"] = trade_date.strftime("%Y%m%d").encode('utf-8')
  df['Timestamp'] =df['Timestamp'].apply(lambda x: trade_date.strftime("%Y-%m-%d").encode('utf-8') + x[10:])
  kwargs = {}


  function_list =  args.function.split(",")
  function_cnts = {x:0 for x in set(function_list)}
  for function_name in function_list:
    if not function_name in ["NBBOPrice", "VWAP", "ROD"]:
      raise Exception("Unsupported function:{}".format(function_name))
    if function_name == "ROD" and len(function_list) > 1:
      raise Exception("Executing of ROD function bundled with other function is not supported".format(function_name))

  function_args = {}
  for function_name in function_cnts.keys():
    function_args[function_name] = { name:dtype for name,dtype,req in taqpy.ArgumentList(function_name) }

  argument_mapping = []
  for function_name in function_list:
    arguments = []
    function_cnts[function_name] += 1
    alias = "{}_{}".format(function_name, function_cnts[function_name])
    for arg, dtype in function_args[function_name].items():
      if arg in df.columns:
        # VWAP only: EndTime and Markouts are mutually exclusive
        if function_name == "VWAP":
          if (arg == "EndTime" and args.markouts > 0) or (arg == "Markouts" and args.markouts == 0):
            continue;
        arguments.append((arg, arg))
        if not arg in kwargs.keys():
          kwargs[arg] = np.array(df[arg], dtype=function_args[function_name][arg])
    argument_mapping.append({"function" : function_name, "alias" : alias, "arguments" : arguments})
  hdr = {}
  hdr["request_id"] = str(uuid.uuid1())
  hdr["service"] = taqproc
  hdr["input_cnt"] = len(df)
  hdr["input_sorted"] = True
  hdr["time_zone"] = "America/New_York"
  hdr["argument_mapping"] = argument_mapping
  request = json.dumps(hdr)

  #print(df.head())
  #print(request)
  global query_time
  started = datetime.datetime.now()
  ret = taqpy.Execute(request, **kwargs)
  query_time = datetime.datetime.now() - started
  # list is returned
  # 1st entry is json with execution summary
  #print(ret[0])
  ret_json = json.loads(ret[0])
  # may or may not contain errors
  if "error_summary" not in ret_json.keys() or type(ret_json["error_summary"]) != type([]):
      ret_json["error_summary"] = []
  #print("error_summary {}".format(ret_json["error_summary"]))
  # optionally create result dataframe
  ret_df = None
  if "result_fields" in ret_json.keys() and len(ret) > 1:
      ret_flds = ret_json["result_fields"]
      data = {"ID" : ret[1]} # second member is ID array
      idx = 2;
      for function, function_flds in ret_flds.items():
        for fld_name, fld_type in function_flds:
          data["{}.{}".format(function, fld_name)] = pd.Series(ret[idx])
          idx += 1
      ret_df = pd.DataFrame(data)
      #print (ret_df.head(10))
      #print (ret_df.iloc[0])
      ret_df.to_hdf("out.hdf", key = "perf")



if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-i", "--input-file", type=str, default="taq-proc-test.hdf", help="exec: input hdf file")
  parser.add_argument("-f", "--function", type=str, default="", help="prep: comma-separated list of functions: NBBOPrice VWAP ROD")
  parser.add_argument("-d", "--date", type=str, default="", help="prep: trade date in YYYYMMDD or YYYY-MM-DD formats")
  parser.add_argument("-m", "--markouts", type=int, default=0, help="prep: markouts size; pass zero not to generate markouts")
  try:
    args = parser.parse_args()
    started = datetime.datetime.now()
    Execute(args)
    finished = datetime.datetime.now()
    if query_time:
      print("started:{} finished:{} run-time:{} query-time:{}".format(started, finished, finished - started, query_time))
    else:
      print("started:{} finished:{} run-time:{}".format(started, finished, finished - started))
  except IOError as e:
    if e.errno != 32: # broken pipe
      print("I/O error({0}): {1}".format(e.errno, e.strerror))
  except Exception as ex:
    print("row:{} err:{}".format(row_cnt, ex))
