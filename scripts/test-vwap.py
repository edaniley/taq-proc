# sample to demonstrate TAQ processor Python API

taqproc = "127.0.0.1:3090"
test_size = 3
date = "20200331"
function_name = "VWAP"

# helper routines
import taqpy

print("FunctionList {}".format(taqpy.FunctionList()))
print("ArgumentNames {}".format(taqpy.ArgumentNames(function_name))) # lists argument names and expected formats
print("ArgumentList {}".format(taqpy.ArgumentList(function_name)))
print("ResultFields {}".format(taqpy.ResultFields(function_name)))


# generate sample dataset for VWAP request

import pandas as pd
import datetime
import random
from collections import deque

symbols     = ["BAC","SPY","ACEL+"]
ref_prices  = {"BAC":21.7109, "SPY":259.801, "ACEL+":1.4999}
sides       = ["B","S",""]
flavors     = [1,2,3,4]       # 1 - regular, 2 - TRF, 3 - regular_TRF, 4 - all
volumes     = [100,500,1000]  # target volume
povs        = [.1, .01, .5]   # target participation

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
req_symb = []
req_date = [date] * test_size
req_stime = []
req_etime = []
req_lmtpx = []
req_side    = [sides[random.randrange(0, len(sides))] for i in range(test_size)]
req_flavor  = [flavors[random.randrange(0, len(flavors))] for i in range(test_size)]
req_tvol    = [volumes[random.randrange(0, len(volumes))] for i in range(test_size)]
req_tpov    = [povs[random.randrange(0, len(povs))] for i in range(test_size)]
req_ticks   = [random.choice([5,10,-5,50]) for i in range(test_size)]


stime.sort()
for i in range(len(stime)):
    symbol = symbols[symbols_idx[0]]
    start_time = str(trade_date + datetime.timedelta(microseconds=stime[i])).split()[1]
    end_time = str(trade_date + datetime.timedelta(microseconds=min(stime[i] + dtime[i], USEC_MAX))).split()[1]
    times_by_symbol[symbol].append((start_time, end_time))
    symbols_idx.rotate(1)

i = 0
for symbol in symbols:
    for times in times_by_symbol[symbol]:
        req_symb.append(symbol)
        req_stime.append(times[0])
        req_etime.append(times[1])
        limit_price = ref_prices[symbol] if req_side[i] == 'B' or req_side[i] == 'S' else 0
        req_lmtpx.append(limit_price)
        i += 1

req_df = pd.DataFrame({"Symbol" : pd.Series(req_symb)
                      ,"Date" : pd.Series(req_date)
                      ,"StartTime" : pd.Series(req_stime)
                      ,"EndTime" : pd.Series(req_etime)
                      ,"Side" : pd.Series(req_side)
                      ,"LimitPx" : pd.Series(req_lmtpx)
                      ,"Flavor" : pd.Series(req_flavor)
                      ,"TargetVolume" : pd.Series(req_tvol)
                      ,"TargetPOV" : pd.Series(req_tpov)
                      ,"Ticks" : pd.Series(req_ticks)
                      })
print(req_df.head())


# validate input

if function_name not in taqpy.FunctionList():
    raise Exception("Unknown function")

for col in req_df.columns:
    found = False
    for arg in taqpy.ArgumentList(function_name):
        if col == arg[0]:
            #print("found {} type {}".format(col, arg[1]))
            found = True
    if not found:
        raise Exception("Function:{} unknown argument:{}".format(function_name, col))

# prepare input

import numpy as np

argument_list = []
kwargs = {}

for arg in taqpy.ArgumentList(function_name):
    for col in req_df.columns:
        if col == arg[0]:
            kwargs[arg[0]] = np.array(req_df[arg[0]], dtype=arg[1])
            argument_list.append(arg[0])
print(argument_list)

# prepare request json

import uuid
import json

hdr = {}
hdr["tcp"] = taqproc
hdr["request_id"] = str(uuid.uuid1())
hdr["function_list"] = [function_name]
hdr["argument_list"] = argument_list
hdr["separator"] = "|"
hdr["input_sorted"] = True
hdr["input_cnt"] = len(req_df)
hdr["output_format"] = "psv"
hdr["time_zone"] = "America/New_York"

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
ret_flds = taqpy.ResultFields(function_name)
if len(ret) > 1:
    data = {}
    for fld in range(len(ret_flds)):
        data[ret_flds[fld][0]] = pd.Series(ret[fld+1])
    ret_df = pd.DataFrame(data)

print (ret_df.head())

