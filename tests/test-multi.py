# sample to demonstrate TAQ processor Python API

taqproc = "127.0.0.1:3090"
test_size = 8
date = "20200331"

# helper routines
import taqpy

#taqpy.Describe()
#print("FunctionList {}".format(taqpy.FunctionList()))
#for function_name in taqpy.FunctionList():
#  print("ArgumentNames {}".format(taqpy.ArgumentNames(function_name))) # lists argument names and expected formats
#  print("ArgumentList {}".format(taqpy.ArgumentList(function_name)))
#  print("ResultFields {}".format(taqpy.ResultFields(function_name)))


# generate sample dataset for VWAP request

import sys
import pandas as pd
import datetime
import random
from collections import deque

symbols     = ["BAC","SPY","ACEL+"]
ref_prices  = {"BAC":21.7109, "SPY":259.801, "ACEL+":1.4999}
sides       = ["B","S",""]
flavors     = [1,2,3,4,5]     # 1 - Regular exchange, 2 - Regular TRF, 3 - Regular, 4 - Block, 5 - All
volumes     = [100,500,1000]  # target volume
povs        = [.1, .01, .5]   # target participation
markouts    = "10t,1s"#,45s,0.5h"

USEC_MIN = 1000000*(9*3600+30*60)
USEC_MAX = 1000000*(16*3600)
SPAN_MIN = 1000000          # 1 sec
SPAN_MAX = 1000000*(4*3600) # 4 hr

trade_date =  datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]))
symbols_idx = deque(range(0, len(symbols)))
times_by_symbol={}
for symbol in symbols:
    times_by_symbol[symbol]=[]
stime = [random.randrange(USEC_MIN, USEC_MAX) for i in range(test_size)]
dtime = [random.randrange(SPAN_MIN, SPAN_MAX) for i in range(test_size)]

req_symb  = []
# NBBO
req_time  = []
req_xtime = [] # execution timestamp
# VWAP
req_date  = [date] * test_size
req_stime = [] # start time
req_etime = [] # end time
req_lmtpx = [] # limit price
req_side      = [sides[random.randrange(0, len(sides))] for i in range(test_size)]
req_flavor    = [flavors[random.randrange(0, len(flavors))] for i in range(test_size)]
req_tvol      = [volumes[random.randrange(0, len(volumes))] for i in range(test_size)]
req_tpov      = [povs[random.randrange(0, len(povs))] for i in range(test_size)]
req_ticks     = [random.choice([5,10,-5,50]) for i in range(test_size)]
req_markouts  = [markouts] * test_size


stime.sort()
for i in range(len(stime)):
    symbol = symbols[symbols_idx[0]]
    timestamp = trade_date + datetime.timedelta(microseconds=stime[i])
    start_time = str(trade_date + datetime.timedelta(microseconds=stime[i])).split()[1]
    end_time = str(trade_date + datetime.timedelta(microseconds=min(stime[i] + dtime[i], USEC_MAX))).split()[1]
    times_by_symbol[symbol].append((timestamp, start_time, end_time))
    symbols_idx.rotate(1)

i = 0
for symbol in symbols:
    for tstamp,start_t,end_t in times_by_symbol[symbol]:
        req_symb.append(symbol)
        req_time.append(tstamp.isoformat('T'))
        req_xtime.append((tstamp+datetime.timedelta(seconds=60)).isoformat('T'))
        req_stime.append(start_t)
        req_etime.append(end_t)
        limit_price = ref_prices[symbol] if req_side[i] == 'B' or req_side[i] == 'S' else 0
        req_lmtpx.append(limit_price)
        i += 1

req_df = pd.DataFrame({"symbol" : pd.Series(req_symb)
                      ,"arrival_ts" : pd.Series(req_time)
                      ,"exec_ts" : pd.Series(req_xtime)
                      ,"markouts" : pd.Series(req_markouts)
                      ,"trade_date" : pd.Series(req_date)
                      ,"start_t" : pd.Series(req_stime)
                      ,"side" : pd.Series(req_side)
                      ,"limit" : pd.Series(req_lmtpx)
                      ,"flavor" : pd.Series(req_flavor)
                      ,"end_t" : pd.Series(req_etime)
                      ,"targvol" : pd.Series(req_tvol)
                      ,"targpov" : pd.Series(req_tpov)
                      ,"ticks" : pd.Series(req_ticks)
                      })
print(req_df.head(10))
print(req_df.iloc[0])


# prepare input

import numpy as np

# build argument list
argument_mapping = []
# NBBOPrice
argument_mapping.append({
  "function" : "NBBOPrice",
  "alias" : "arrival",
  "arguments" : [
    ("Symbol","symbol"),        # pass column 'symbol' as 'Symbol' argument for NBBOPrice function
    ("Timestamp","arrival_ts")  # pass column 'arrival_ts' as 'Timestamp' argument
  ]
})
argument_mapping.append({
  "function" : "NBBOPrice",
  "alias" : "post-exec",
  "arguments" : [
    ("Symbol","symbol"),        # pass column 'symbol' as 'Symbol' argument for NBBOPrice function
    ("Timestamp","exec_ts"),    # pass column 'exec_ts' as 'Timestamp' argument
    ("Markouts", "markouts")    # pass column 'markouts' as 'Markouts' argument
  ]
})

# same for VWAP
argument_mapping.append({
  "function" : "VWAP",
  "alias" : "vwap",
  "arguments" : [
    ("Symbol","symbol"),
    ("Date","trade_date"),
    ("StartTime","start_t"),
    ("Side","side"),
    ("LimitPx","limit"),
    ("Flavor","flavor"),
    #("EndTime","end_t"),
    #("TargetVolume","targvol"),
    #("TargetPOV","targpov"),
    #("Ticks","ticks"),
    ("Markouts", "markouts")
  ]
})

print(argument_mapping)

# taqpy.ArgumentList("*") unique list of all arguments for entire library of available functions
arg_types = {name:dtype for name,dtype,required in taqpy.ArgumentList("*")}

kwargs = {}
kwargs['some-other-stuff'] = None  # will be ignored because not in argument_mapping list
for function_args in argument_mapping:
  function_name = function_args["function"]
  alias = function_args["alias"]
  for arg_name, col_name in function_args["arguments"]:
    if not col_name in kwargs.keys():
      if arg_name in arg_types.keys():
        kwargs[col_name] = np.array(req_df[col_name], dtype=arg_types[arg_name])
      else:
        raise Exception("Function:{} unknown argument:{}".format(function_name, arg_name))

# prepare request json
import uuid
import json

hdr = {}
hdr["request_id"] = str(uuid.uuid1())
hdr["service"] = taqproc
hdr["input_cnt"] = len(req_df)
hdr["input_sorted"] = True
hdr["time_zone"] = "America/New_York"
hdr["argument_mapping"] = argument_mapping

req_json = json.dumps(hdr)
print(req_json)

# execute query

ret = taqpy.Execute(req_json, **kwargs)
# list is returned
# 1st entry is json with execution summary
print(ret[0])
ret_json = json.loads(ret[0])

# may or may not contain errors
if "error_summary" not in ret_json.keys() or type(ret_json["error_summary"]) != type([]):
    ret_json["error_summary"] = []
print("error_summary {}".format(ret_json["error_summary"]))

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
    print (ret_df.head(10))
    print (ret_df.iloc[0])

print("")


