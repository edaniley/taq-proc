import subprocess
import tempfile
import uuid
import json
import numpy as np
import pandas as pd
import taqpy
from datetime import datetime
from datetime import time
from datetime import date

symbols = []
quotes = {}
requests = {}
results = {}

def AddSymbol(symbol : str, **kwargs):
  global symbols
  Tape = "A" if "Tape" not in kwargs.keys() else kwargs["Tape"]
  Listed_Exchange = "N" if "Listed_Exchange" not in kwargs.keys() else kwargs["Listed_Exchange"]
  Round_Lot = "100" if "Round_Lot" not in kwargs.keys() else str(kwargs["Round_Lot"])
  SIP_Symbol = symbol if "SIP_Symbol" not in kwargs.keys() else kwargs["SIP_Symbol"]
  base = "{}|Test symbol|CUSIP|A|{}|X|Y|{}|{}|1|{}|100|10000| |    |    |  |  |1|1|1|1|1|1|1|1|1|1|1|1|1|1|1|1| |"
  # Symbol|Security_Description|CUSIP|Security_Type|SIP_Symbol|Old_Symbol|Test_Symbol_Flag|Listed_Exchange
  # |Tape|Unit_Of_Trade|Round_Lot|NYSE_Industry_Code|Shares_Outstanding|Halt_Delay_Reason
  # |Specialist_Clearing_Agent|Specialist_Clearing_Number|Specialist_Post_Number|Specialist_Panel
  # |TradedOnNYSEMKT|TradedOnNASDAQBX|TradedOnNSX|TradedOnFINRA|TradedOnISE|TradedOnEdgeA|TradedOnEdgeX
  # |TradedOnCHX|TradedOnNYSE|TradedOnArca|TradedOnNasdaq|TradedOnCBOE|TradedOnPSX|TradedOnBATSY|TradedOnBATS
  # |TradedOnIEX|Tick_Pilot_Indicator|Effective_Date
  rec = base.format(symbol, SIP_Symbol, Listed_Exchange, Tape, Round_Lot)
  symbols.append(rec)

def MakeSecmaster(yyyymmdd : str):
  global symbols
  data = "\n".join(symbols)
  tmp = tempfile.NamedTemporaryFile(mode='w')
  tmp.write(data)
  tmp.flush()
  cmd = "taq-prep -t master -d {} -i {} ".format(yyyymmdd, tmp.name)
  proc = subprocess.run(cmd,shell=True, capture_output=True)
  tmp.close()
  symbols = []

def ToTaqTime(timestamp) -> str:
  std_fmt = "%H:%M:%S.%f"
  if isinstance(timestamp, datetime):
    return timestamp.strftime(std_fmt)
  elif isinstance(timestamp, time):
    return timestamp.strftime(std_fmt)
  ts = None
  try:
    ts = datetime.strptime(timestamp, std_fmt)
  except:
    pass
  if not ts:
    ts = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
  return ts.strftime(std_fmt)

def AddQuote(symbol : str, timestamp, bid : float, offer: float, **kwargs):
  # Time|Exchange|Symbol|Bid_Price|Bid_Size|Offer_Price|Offer_Size|Quote_Condition|Sequence_Number
  # |National_BBO_Ind|FINRA_BBO_Indicator|FINRA_ADF_MPID_Indicator|Quote_Cancel_Correction
  # |Source_Of_Quote|Retail_Interest_Indicator|Short_Sale_Restriction_Indicator|LULD_BBO_Indicator
  # |SIP_Generated_Message_Identifier|National_BBO_LULD_Indicator|Participant_Timestamp|FINRA_ADF_Timestamp
  # |FINRA_ADF_Market_Participant_Quote_Indicator|Security_Status_Indicator
  global quotes
  taq_time = ToTaqTime(timestamp)
  ts = datetime.strptime(taq_time, "%H:%M:%S.%f")
  Time = ts.strftime("%H%M%S%f000")
  Bid_Size = 1 if "Bid_Size" not in kwargs.keys() else int(kwargs["Bid_Size"])
  Offer_Size = 1 if "Offer_Size" not in kwargs.keys() else int(kwargs["Offer_Size"])
  Exchange = "N" if "Exchange" not in kwargs.keys() else kwargs["Exchange"]
  Quote_Condition = " " if "Quote_Condition" not in kwargs.keys() else kwargs["Quote_Condition"]
  Source_Of_Quote = "C" if "Source_Of_Quote" not in kwargs.keys() else kwargs["Source_Of_Quote"]
  base = "{}|{}|{}|{}|{}|{}|{}|{}|0| | | | |{}| | | | | | | | | "
  rec = base.format(Time, Exchange, symbol, bid, Bid_Size, offer, Offer_Size, Quote_Condition, Source_Of_Quote)
  symb_grp = symbol[:1]
  if not symb_grp in quotes:
    quotes[symb_grp] = []
  quotes[symb_grp].append((ts.time(), rec))

def MakeSymbolQuotes(yyyymmdd : str, symb_grp : str, quote_list):
  data = "\n".join(quote_list)
  tmp = tempfile.NamedTemporaryFile(mode='w')
  tmp.write(data)
  tmp.flush()
  cmd = "taq-prep -t quote -d {} -s {} -i {} ".format(yyyymmdd, symb_grp, tmp.name)
  proc = subprocess.run(cmd,shell=True, capture_output=True)
  tmp.close()

def MakeQuotes(yyyymmdd : str):
  global quotes
  for k, v in quotes.items():
    v.sort()
    symb_quotes = [ x[1] for x in v ]
    MakeSymbolQuotes(yyyymmdd, k,  symb_quotes)
  quotes = {}

def AddFunctionRequest(**kwargs):
  global requests
  function_name = kwargs["function_name"]
  if not function_name in requests.keys():
    requests[function_name] = []
  args = {}
  for argument_name in taqpy.ArgumentNames(function_name):
    args[argument_name] = kwargs[argument_name]
  requests[function_name].append(args)

def AddRequest(**kwargs):
  if 'function_name' not in kwargs.keys():
    raise Exception("Missing  function")
  function_name = kwargs["function_name"]
  if function_name not in taqpy.FunctionList():
    raise Exception("Unknown function")
  for argument_name in taqpy.ArgumentNames(function_name):
    if not argument_name in kwargs.keys():
      raise Exception("Function:{} missing argument:{}".format(function_name, argument_name))
  AddFunctionRequest(**kwargs)

def ExecuteFunction(function_name:str, yyyymmdd:str, tz="America/New_York"):
  global results
  kwargs = {}
  for field in taqpy.ArgumentList(function_name):
    argv = [d[field[0]] for d in requests[function_name]]
    kwargs[field[0]] = np.array(argv, dtype=field[1])
  hdr = {}
  hdr["tcp"] = "127.0.0.1:3090"
  hdr["request_id"] = str(uuid.uuid1())
  hdr["function_list"] = [function_name]
  hdr["argument_list"] = taqpy.ArgumentNames(function_name)
  hdr["separator"] = "|"
  hdr["input_sorted"] = False
  hdr["input_cnt"] = len(requests[function_name])
  hdr["output_format"] = "psv"
  hdr["time_zone"] = tz

  ret = taqpy.Execute(json.dumps(hdr), **kwargs)
  ret_json = json.loads(ret[0])
  if "error_summary" not in ret_json.keys() or type(ret_json["error_summary"]) != type([]):
    ret_json["error_summary"] = []

  df = None
  if len(ret) > 1:
    data = {}
    col = 1
    for field in taqpy.ResultFields(function_name):
      data[field[0]] = pd.Series(ret[col])
      col += 1
    df = pd.DataFrame(data)

  results[function_name] = (ret_json, df)

def ExecuteRequests(yyyymmdd:str, tz="America/New_York"):
  global requests, results
  results = {}
  for function_name in requests.keys():
    ExecuteFunction(function_name, yyyymmdd, tz)
  requests = {}
  return results

