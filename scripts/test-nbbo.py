# sample to demonstrate TAQ processor Python API

taqproc = "127.0.0.1:3090"
test_size = 8
date = "20200331"
function_name = "NBBOPrice"

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
markouts    = "10t,100ms,45s,0.5h"

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
req_symb = []
req_time = []
req_markouts = [markouts] * test_size


stime.sort()
for i in range(len(stime)):
    symbol = symbols[symbols_idx[0]]
    timestamp = trade_date + datetime.timedelta(microseconds=stime[i])
    #print("{}|{}".format(symbol,timestamp.isoformat('T')))
    times_by_symbol[symbol].append(timestamp)
    symbols_idx.rotate(1)

i = 0
for symbol in symbols:
    for tstamp in times_by_symbol[symbol]:
        req_symb.append(symbol)
        req_time.append(tstamp.isoformat('T'))
        i += 1

req_df = pd.DataFrame({"Symbol" : pd.Series(req_symb)
                      ,"Timestamp" : pd.Series(req_time)
                      #,"Markouts" : pd.Series(req_markouts)
                      })
print(req_df.head(10))


# validate input

if function_name not in taqpy.FunctionList():
    raise Exception("Unknown function")

for col in req_df.columns:
    found = False
    for arg in taqpy.ArgumentList(function_name):
        if col == arg[0]:
            found = True
            break
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
ret_flds = ret_json["output_fields"]
if len(ret) > 1:
    data = {}
    for fld in range(len(ret_flds)):
        data[ret_flds[fld]] = pd.Series(ret[fld+1])
    ret_df = pd.DataFrame(data)
    print (ret_df.head(10))

print("")

