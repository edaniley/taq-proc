import sys
import datetime
import argparse
import random
import uuid
from collections import deque

supported_functions = ["Quote", "ROD"]
USEC_MIN = 1000000*(9*3600+30*60)
USEC_MAX = 1000000*(16*3600)
trade_date = None
market_open_time = None
market_close_time = None
row_cnt = 0
argument_list = {
  'Quote' : '"Symbol", "Timestamp"',
  'ROD' : '"Symbol", "Date", "StartTime", "EndTime", "Side", "OrdQty", "LimitPrice", "MPA"',
}

def MakeRequestJson(function, test_size):
  print('{')
  print('  "request_id" : "{}",'.format(uuid.uuid1()))
  print('  "separator" : "|",')
  print('  "response_format" : "psv",')
  print('  "function_list" : [ "{}"],'.format(function))
  print('  "argument_list" : [ {} ],'.format(argument_list[function]))
  print('  "input_sorted" : true,')
  print('  "input_cnt" : {}'.format(test_size))
  print('}')

def MakeQuote(args):
  symbols = []
  if args.symbol_list:
    symbols = args.symbol_list.split(",")
  elif args.symbol_file:
    f = open(args.symbol_file)
    line = f.readline()
    while line:
      symbols.append(line.rstrip("\n"))
      line = f.readline()

  if len(symbols)==0:
    raise Exception("Empty symbol list")
  #symbols.sort()
  symbols_idx = deque(range(0, len(symbols)))
  times_by_symbol={}
  for symbol in symbols:
    times_by_symbol[symbol]=[]
  usec = [random.randrange(USEC_MIN, USEC_MAX) for i in range(args.test_size)]
  usec.sort()
  for delta in usec:
    symbol = symbols[symbols_idx[0]]
    times_by_symbol[symbol].append(trade_date + datetime.timedelta(microseconds=delta))
    symbols_idx.rotate(1)
  record_cnt = 0
  for symbol in symbols:
    for trade_time in times_by_symbol[symbol]:
      record_cnt += 1
      if record_cnt > args.test_size:
          return
      print("{}|{}".format(symbol,trade_time.isoformat('T')))

def MakeROD(args):
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

def main(args):
  global trade_date
  global market_open_time
  global market_close_time

  if len(args.date) == 8:
    trade_date =  datetime.datetime(int(args.date[0:4]), int(args.date[4:6]), int(args.date[6:8]))
  elif len(args.date) == 10:
    trade_date =  datetime.datetime(int(args.date.split("-")[0]), int(args.date.split("-")[1]), int(args.date.split("-")[2]))
  else:
    raise Exception("Unknown Date format")
  market_open_time = trade_date + datetime.timedelta(microseconds=USEC_MIN)
  market_close_time = trade_date + datetime.timedelta(microseconds=USEC_MAX)
  MakeRequestJson(args.function, args.test_size)
  if args.function == "Quote":
    MakeQuote(args)
  elif args.function == "ROD":
    MakeROD(args)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-f", "--function", type=str, default="", help="function name : Quote ROD")
  parser.add_argument("-s", "--symbol-list", type=str, default="", help="comma-separated list of symbols")
  parser.add_argument("-l", "--symbol-file", type=str, default="", help="input file with list of symbols")
  parser.add_argument("-d", "--date", type=str, default="", help="trade date in YYYYMMDD or YYYY-MM-DD formats")
  parser.add_argument("-t", "--test-size", type=int, default=10000, help="test size in records")
  parser.add_argument("-i", "--input-file", type=str, default="", help="for ROD; input file as output from tick-calc")
  try:
    args = parser.parse_args()
    if args.function not in supported_functions:
      print("Unsupported function:{}".format(args.function))
    else:
      started = datetime.datetime.now()
      main(args)
      finished = datetime.datetime.now()
      #print("started:{} finished:{} run-time:{}".format(started, finished, finished - started))
  except IOError as e:
    if e.errno != 32: # broken pipe
      print("I/O error({0}): {1}".format(e.errno, e.strerror))
  except Exception as ex:
    print("row:{} err:{}".format(row_cnt, ex))
