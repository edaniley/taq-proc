import sys
import argparse
from datetime import datetime
from dateutil import tz
import uuid
import json
import socket

in_order_id = 1
in_symbol = 2
in_side = 3
in_entry_time = 4
in_end_time = 5
in_limit_price = 6
in_order_quantity = 7
in_max_wpa = 8
in_resting_eligible = 9
in_execution_id = 10
in_lastsize = 11
in_lastprice = 12
in_time = 13
in_cid = 14
in_date = 15
in_mpid = 16

line_no = 0

def StringToDuration(val):
  tmp = val.split('.')
  if len(tmp) == 1:
    val += ".000000"
  elif len(tmp[1]) > 6:
    to_trim = len(tmp[1]) - 6
    val = val[:len(val)-(len(tmp[1]) - 6)]
  elif len(tmp[1]) < 6:
    val = tmp[0] + '.' + '{:<06s}'.format(tmp[1])
  dt = datetime.strptime(val, "%H:%M:%S.%f")
  return timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond)

def TestROD(args):
  global line_no
  argument_mapping = []
  argument_mapping.append({
    "function" : "ROD",
    "alias" : "rod",
    "arguments" : [
      ("OrdID", 1),
      ("Symbol", 2),
      ("Date", 3),
      ("StartTime", 4),
      ("EndTime", 5),
      ("Side", 6),
      ("OrdQty", 7),
      ("LimitPx", 8),
      ("MPA", 9),
      ("ExecTime", 10),
      ("ExecQty", 11)
    ]
  })
  hdr = {}
  hdr["request_id"] = str(uuid.uuid1())
  hdr["input_cnt"] = args.size
  hdr["input_sorted"] = False
  hdr["time_zone"] = "UTC"
  hdr["argument_mapping"] = argument_mapping
  req_json = json.dumps(hdr)
  req_json += "\n"

  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  addr, port = args.tcp.split(":")
  sock.connect((addr, int(port)))
  sock.send(req_json.encode("utf-8"))

  file = open(args.in_file)
  line = file.readline()
  line_no = 1
  while line:
    rec = line.rstrip("\n").split(",")
    if len(rec[0])>0:
      OrdID = "{}.{}".format(rec[in_mpid], rec[in_order_id])
      Symbol = rec[in_symbol]
      Date = rec[in_date]
      StartTime = rec[in_entry_time].split()[1].split("+")[0] if len(rec[in_entry_time]) > 0 else ""
      EndTime = rec[in_end_time].split()[1].split("+")[0] if len(rec[in_end_time]) > 0 else ""
      Side = rec[in_side][:1] if len(rec[in_side]) > 0 else ""
      OrdQty = rec[in_order_quantity]
      LimitPx = rec[in_limit_price]
      MPA = rec[in_max_wpa]
      ExecQty = rec[in_lastsize]
      ExecTime = rec[in_time].split()[1].split("+")[0] if len(ExecQty) > 0 else ""
      out_rec = "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}\n".format(
          OrdID, Symbol, Date, StartTime, EndTime, Side, OrdQty, LimitPx, MPA, ExecQty, ExecTime)
      sock.send(out_rec.encode("utf-8"))
    line = file.readline()
    line_no += 1

  sockFile = sock.makefile()
  line = sockFile.readline()
  res = json.loads(line)
  output_cnt = int(res["output_records"])
  while line and output_cnt  > 0:
    line = sockFile.readline()
    output_cnt -= 1
    #print(line[:-1])
  line = sockFile.readline()
  return json.loads(line)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-i", "--in-file", type=str, default="sample.csv", help="input csv file")
  parser.add_argument("-t", "--tcp", type=str, default="127.0.0.1:3090", help="server address:port")
  parser.add_argument("-s", "--size", type=int, default=0, help="number of records in input file")
  try:
    args = parser.parse_args()
    started = datetime.now()
    runtime_summary = TestROD(args)
    finished = datetime.now()
    if runtime_summary:
      request_parsing_sorting = StringToDuration(runtime_summary["request_parsing_sorting"])
      execution = StringToDuration(runtime_summary["execution"])
      result_merging_sorting = StringToDuration(runtime_summary["result_merging_sorting"])
      print("started={} finished={} run-time={} request_parsing_sorting={} execution={} result_merging_sorting={}".format(
        started, finished, finished - started, request_parsing_sorting, execution, result_merging_sorting))
    else:
      print("started:{} finished:{} query-time:{}".format(started, finished, finished - started))
  except IOError as e:
    if e.errno != 32: # broken pipe
      print("I/O error({0}): {1}".format(e.errno, e.strerror))
  except Exception as ex:
    print("err:{} line_no:{}".format(ex, line_no))
