import subprocess
import tempfile
import uuid
import json
import numpy as np
import taqpy
from datetime import datetime
from datetime import time
from datetime import date

symbols = []
quotes = {}
trades = {}
requests = {}
results = {}
strict_functions = ["ROD", "Quote"]

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
  cmd = "taq-prep -t master -d {} -i {} -o /tmp".format(yyyymmdd, tmp.name)
  proc = subprocess.run(cmd,shell=True)
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
  cmd = "taq-prep -t quote -d {} -s {} -i {} -o /tmp".format(yyyymmdd, symb_grp, tmp.name)
  proc = subprocess.run(cmd,shell=True)
  tmp.close()

def MakeQuotes(yyyymmdd : str):
  global quotes
  for k, v in quotes.items():
    v.sort()
    symb_quotes = [ x[1] for x in v ]
    MakeSymbolQuotes(yyyymmdd, k,  symb_quotes)
  quotes = {}

def AddTrade(symbol : str, timestamp, trd_vol : int, trd_px: float, **kwargs):
  # Time|Exchange|Symbol|Sale_Condition|Trade_Volume|Trade_Price}
  # Trade_Stop_Stock_Indicator|Trade_Correction_Indicator|Sequence_Number|Trade_Id|
  # Source_of_Trade|Trade_Reporting_Facility|Participant_Timestamp|
  # Trade_Reporting_Facility_TRF_Timestamp|Trade_Through_Exempt_Indicator
  global trades
  taq_time = ToTaqTime(timestamp)
  ts = datetime.strptime(taq_time, "%H:%M:%S.%f")
  Time = ts.strftime("%H%M%S%f000")
  Exchange = "N" if "Exchange" not in kwargs.keys() else str(kwargs["Exchange"])
  Sale_Condition = "" if "Sale_Condition" not in kwargs.keys() else str(kwargs["Sale_Condition"])
  Sale_Condition = Sale_Condition.strip()[:4]
  Sale_Condition = Sale_Condition+' '*(4-len(Sale_Condition))
  Trade_Correction_Indicator = "00" if "Trade_Correction_Indicator" not in kwargs.keys() else str(kwargs["Trade_Correction_Indicator"])
  Source_of_Trade = "C" if "Source_of_Trade" not in kwargs.keys() else str(kwargs["Source_of_Trade"])
  Trade_Reporting_Facility = "N" if "Trade_Reporting_Facility" not in kwargs.keys() else str(kwargs["Trade_Reporting_Facility"])
  Trade_Through_Exempt_Indicator = "0" if "Trade_Through_Exempt_Indicator" not in kwargs.keys() else str(kwargs["Trade_Through_Exempt_Indicator"])
  base = "{}|{}|{}|{}|{}|{}| |{}| | |{}|{}| | |{}"
  rec = base.format(Time, Exchange, symbol, Sale_Condition, trd_vol, trd_px,
                    Trade_Correction_Indicator, Source_of_Trade, Trade_Reporting_Facility,
                    Trade_Through_Exempt_Indicator)
  if not symbol in trades:
    trades[symbol] = []
  trades[symbol].append((ts.time(), rec))

def MakeTrades(yyyymmdd : str):
  global trades
  tmp = tempfile.NamedTemporaryFile(mode='w')
  for k, v in trades.items():
    v.sort()
    symb_trades = [ x[1] for x in v ]
    data = "\n".join(symb_trades)
    tmp.write(data)
    tmp.flush()
  cmd = "taq-prep -t trade -d {} -i {} -o /tmp".format(yyyymmdd, tmp.name)
  proc = subprocess.run(cmd,shell=True)
  tmp.close()
  trades = {}

def AddFunctionRequest(**kwargs):
  global requests
  function_name = kwargs["function_name"]
  if not function_name in requests.keys():
    requests[function_name] = []
  args = {}
  for argument_name in taqpy.ArgumentNames(function_name):
    if argument_name in kwargs.keys():
      args[argument_name] = kwargs[argument_name]
  requests[function_name].append(args)

def AddRequest(**kwargs):
  if 'function_name' not in kwargs.keys():
    raise Exception("Missing  function")
  function_name = kwargs["function_name"]
  if function_name not in taqpy.FunctionList():
    raise Exception("Unknown function")
  if function_name in strict_functions:
    for argument_name in taqpy.ArgumentNames(function_name):
      if not argument_name in kwargs.keys():
        raise Exception("Function:{} missing argument:{}".format(function_name, argument_name))
  AddFunctionRequest(**kwargs)

def ExecuteFunction(function_name:str, yyyymmdd:str, tz="America/New_York"):
  global results
  kwargs = {}
  arg_list = []
  req_args = [k for k in requests[function_name][0].keys()]
  for arg in taqpy.ArgumentList(function_name):
    if arg[0] in req_args:
      arg_list.append(arg)
  for arg_name,arg_type in arg_list:
    lst = [arr[arg_name] for arr in requests[function_name]]
    kwargs[arg_name] = np.array(lst, dtype=arg_type)

  hdr = {}
  hdr["tcp"] = "127.0.0.1:3090"
  hdr["request_id"] = str(uuid.uuid1())
  hdr["function_list"] = [function_name]
  hdr["argument_list"] = [arg[0] for arg in arg_list]
  hdr["separator"] = "|"
  hdr["input_sorted"] = False
  hdr["input_cnt"] = len(requests[function_name])
  hdr["output_format"] = "psv"
  hdr["time_zone"] = tz

  ret = taqpy.Execute(json.dumps(hdr), **kwargs)
  ret_json = json.loads(ret[0])
  if "error_summary" not in ret_json.keys() or type(ret_json["error_summary"]) != type([]):
    ret_json["error_summary"] = []

  arrs = { col_name[0] : ret[idx+1] for idx, col_name in enumerate(taqpy.ResultFields(function_name)) }
  results[function_name] = (ret_json, arrs)

def ExecuteRequests(yyyymmdd:str, tz="America/New_York"):
  global requests, results
  results = {}
  for function_name in requests.keys():
    ExecuteFunction(function_name, yyyymmdd, tz)
  requests = {}
  return results

