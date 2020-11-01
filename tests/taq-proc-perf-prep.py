import sys
import datetime
import argparse
import random
import uuid
from collections import deque
import pandas as pd
import numpy as np
import taqpy

#df = pd.read_hdf(R'C:\Users\Ed\src\taq-proc\data\sample.hdf')
supported_functions = ["NBBOPrice", "VWAP", "ROD"]
USEC_MIN = 1000000*(9*3600+30*60)
USEC_MAX = 1000000*(16*3600)
SPAN_MIN = 1000000          # 1 sec
SPAN_MAX = 1000000*(4*3600) # 4 hr
trade_date = None
market_open_time = None
market_close_time = None
row_cnt = 0

arg_def = {}#name:dtype for name,dtype,req in taqpy.ArgumentList("VWAP")}
for name,dtype,req in taqpy.ArgumentList("VWAP"):
  arg_def[name] = dtype
for name,dtype,req in taqpy.ArgumentList("NBBOPrice"):
  arg_def[name] = dtype
for name,dtype,req in taqpy.ArgumentList("ROD"):
  arg_def[name] = dtype


def ReadSymbolsFromFile(input_file):
  if len(input_file) == 0:
    raise Exception("Missing symbol file path")
  symbols = [] # list of tuples (symbol, reference price)
  f = open(input_file)
  line = f.readline()
  while line:
    symbols.append(line.rstrip("\n").split(","))
    line = f.readline()
  f.close()
  return symbols


def Prepare(args):
  symbols  = ReadSymbolsFromFile(args.symbol_file)
  sides    = ["B","S","B","S", ""]
  markouts = ["10t","100ms","1s","-5t","45s","0.5h","100t","-5s"]
  if len(symbols) == 0:
    raise Exception("Empty symbol list")

  if len(args.date) == 8:
    trade_date =  datetime.datetime(int(args.date[0:4]), int(args.date[4:6]), int(args.date[6:8]))
  elif len(args.date) == 10:
    trade_date =  datetime.datetime(int(args.date.split("-")[0]), int(args.date.split("-")[1]), int(args.date.split("-")[2]))
  else:
    raise Exception("Unknown Date format")
  market_open_time = trade_date + datetime.timedelta(microseconds=USEC_MIN)
  market_close_time = trade_date + datetime.timedelta(microseconds=USEC_MAX)

  symbols_idx = deque(range(0, len(symbols)))
  times_by_symbol={}
  for symbol, ref_price in symbols:
    times_by_symbol[symbol]=[]
  stime = [random.randrange(USEC_MIN, USEC_MAX) for i in range(args.test_size)]
  dtime = [random.randrange(SPAN_MIN, SPAN_MAX) for i in range(args.test_size)]
  rside = [sides[random.randrange(0, len(sides))] for i in range(args.test_size)]
  stime.sort()
  for i in range(len(stime)):
    symbol = symbols[symbols_idx[0]][0]
    start_time = trade_date + datetime.timedelta(microseconds=stime[i])
    end_time = trade_date + datetime.timedelta(microseconds=min(stime[i] + dtime[i], USEC_MAX))
    times_by_symbol[symbol].append((start_time, end_time))
    symbols_idx.rotate(1)

  symbol_arr = np.empty(args.test_size, dtype = arg_def["Symbol"])
  side_arr = np.empty(args.test_size, dtype = arg_def["Side"])
  price_arr = np.empty(args.test_size, dtype = arg_def["LimitPx"])
  date_arr = np.full(args.test_size, "20200331", dtype = arg_def["Date"])
  timestamp_arr = np.empty(args.test_size, dtype = arg_def["Timestamp"])
  start_time_arr = np.empty(args.test_size, dtype = arg_def["StartTime"])
  end_time_arr = np.empty(args.test_size, dtype = arg_def["EndTime"])
  markouts_arr = np.full(args.test_size, ",".join(markouts[:args.markouts]), dtype = arg_def["Markouts"])
  i = 0
  for symbol, ref_price in symbols:
    for start_time, end_time in times_by_symbol[symbol]:
      symbol_arr[i] = symbol
      side_arr[i] = rside[i]
      price_arr[i] = ref_price if rside[i] == 'B' or rside[i] == 'S' else 0
      timestamp_arr[i] = start_time.isoformat('T')
      start_time_arr[i] = str(start_time).split()[1]
      end_time_arr[i] = str(end_time).split()[1]
      i += 1
  df = pd.DataFrame(list(range(1,args.test_size+1)), columns=["ID"])
  df["Symbol"] = symbol_arr
  df["Date"] = date_arr
  df["Timestamp"] = timestamp_arr
  df["StartTime"] = start_time_arr
  df["EndTime"] = end_time_arr
  df["Side"] = side_arr
  df["LimitPx"] = price_arr
  if args.markouts > 0:
    df["Markouts"] = markouts_arr
  df.to_hdf(args.out_file, key="perf")

def MakeROD(args, idx:int = 0):
  global row_cnt
  sides = ['B','S']
  mpa_list = ['3','-2', '2', '2', '0', '0', ''] # 2 and 0 occur twice as often
  size_list = [200,300,400, 1000]
  one_min = 60 * 1000000
  fifteen_sec = 15 * 1000000
  file = open(args.input_file)
  line = file.readline()
  while line:
    line.strip()
    row_cnt = row_cnt + 1
    if len(line) > 0:
      row = line.split("|")
      if row[0] == "id" and len(row) == 16:
        symbol = row[3]
        bid = float(row[9])
        ask = float(row[13])
        mpa = mpa_list[random.randrange(0,len(size_list))]
         # unless mpa is not set , one out of ten has no limit price, the rest has limit == mid-point price
        limit = str(round((bid+ask)/2,4)) if random.randrange(0,10) > 0 or len(mpa)==0 else ''
        side = sides[random.randrange(0,2)]
        ord_qty = size_list[random.randrange(0,len(size_list))]
        t = datetime.datetime.strptime(row[5][:15],"%H:%M:%S.%f")
        delta = datetime.timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)
        #order_start_time = trade_date + delta
        order_start_time = delta
        duration = random.randrange(one_min - fifteen_sec, one_min + fifteen_sec) # one minute +/- 15 sec
        #order_end_time = order_start_time + datetime.timedelta(microseconds=duration)
        order_end_time = delta + datetime.timedelta(microseconds=duration)
        #print("{}|{}|{}|{}|{}|{}".format(symbol, order_start_time.isoformat('T'), order_end_time.isoformat('T'),side, limit, peg_type))
        fill_cnt = random.randrange(0,3) # up to 2 executions
        appendix = ""
        cum_qty = 0
        if fill_cnt > 0:
          sub_duration = duration / (fill_cnt + 1)
          for i in range(0, fill_cnt):
            if cum_qty + 100 > ord_qty:
              break
            exec_time = order_start_time + datetime.timedelta(microseconds=(sub_duration *(i+1)))
            exec_qty = 100
            cum_qty += exec_qty
            appendix += "|{}|{}".format(exec_time,exec_qty)

        print("{}|{}|{}|{}|{}|{}|{}|{}{}".format(symbol, trade_date.date(), order_start_time, order_end_time, side, ord_qty, limit, mpa, appendix))

    line = file.readline()

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-s", "--symbol-file", type=str, default="", help="input file with list of symbols")
  parser.add_argument("-d", "--date", type=str, default="", help="trade date in YYYYMMDD or YYYY-MM-DD formats")
  parser.add_argument("-t", "--test-size", type=int, default=10000, help="test size in records")
  parser.add_argument("-m", "--markouts", type=int, default=0, help="markouts size; pass zero not to generate markouts")
  parser.add_argument("-o", "--out-file", type=str, default="taq-proc-test.hdf", help="output file name")
  try:
    args = parser.parse_args()
    started = datetime.datetime.now()
    Prepare(args)
    finished = datetime.datetime.now()
    print("started:{} finished:{} run-time:{}".format(started, finished, finished - started))
  except IOError as e:
    if e.errno != 32: # broken pipe
      print("I/O error({0}): {1}".format(e.errno, e.strerror))
  except Exception as ex:
    print("row:{} err:{}".format(row_cnt, ex))
