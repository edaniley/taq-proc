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
request_fields = {}
request_functions = {}

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
  cmd = "taq-prep -t quote-po -d {} -s {} -i {} -o /tmp".format(yyyymmdd, symb_grp, tmp.name)
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

def VerifyRequest(**kwargs):
  # for every function call verifies that all records have consistent set of input arguments
  # stores argument names of first record and compares them with subsequent records
  global request_functions, request_fields
  alias = kwargs["alias"]
  function_name = kwargs["function_name"]
  fields = []
  for argument_name in taqpy.ArgumentNames(function_name):
    if argument_name in kwargs.keys():
      fields.append(argument_name)

  if alias not in request_functions.keys():
    request_fields[alias] = fields
    request_functions[alias] = function_name
  else:
    if function_name != request_functions[alias]:
      raise Exception("Inconsistent function name {} for alias {}".format(function_name, alias))
    if len(fields) != len(request_fields[alias]):
      raise Exception("Inconsistent number of arguments for function {}({})".format(function_name, alias))
    for fld in request_fields[alias]:
      if fld not in fields:
        raise Exception("Argument {} was not present in previously submitted records for function {}({})".format(function_name, alias))

def AddFunctionRequest(**kwargs):
  global requests
  alias = kwargs["alias"]
  function_name = kwargs["function_name"]
  if not alias in requests.keys():
    requests[alias] = []
  args = {}
  for argument_name in taqpy.ArgumentNames(function_name):
    if argument_name in kwargs.keys():
      args[argument_name] = kwargs[argument_name]
  requests[alias].append(args)

def AddRequest(**kwargs):
  if 'alias' not in kwargs.keys():
    raise Exception("Missing alias ")
  if 'function_name' not in kwargs.keys():
    raise Exception("Missing function name")
  function_name = kwargs["function_name"]
  if function_name not in taqpy.FunctionList():
    raise Exception("Unknown function")
  for argument in taqpy.ArgumentList(function_name):
    argument_name = argument[0]
    argument_required = argument[2]
    if argument_required and not argument_name in kwargs.keys():
      raise Exception("Function:{} missing required argument:{}".format(function_name, argument_name))
  VerifyRequest(**kwargs)
  AddFunctionRequest(**kwargs)

def ExecuteRequests(yyyymmdd:str, tz="America/New_York"):
  global requests, request_functions, request_fields
  kwargs = {}

  input_cnt = 0
  argument_mapping = []
  for alias, input_records in requests.items():
    function_name = request_functions[alias]
    function_arg_map = {"function": function_name, "alias": alias}
    arg_map = []
    rec_cnt = 0
    for arg_name, arg_type, requred in taqpy.ArgumentList(function_name):
      if arg_name in request_fields[alias]:
        input_arg = "{}.{}".format(alias,arg_name)
        arg_map.append((arg_name, input_arg))
        arr = [x[arg_name] for x in input_records]
        kwargs[input_arg] = np.array(arr, dtype=arg_type)
        if rec_cnt == 0:
          rec_cnt = len(arr)
        elif rec_cnt != len(arr):
          raise Exception("Inconsistent number of records")# should not reach this line
      elif requred:
        raise Exception("Missing required argument {} for function {}".format(arg_name, function_name))
    if input_cnt == 0:
      input_cnt = rec_cnt
    elif input_cnt != rec_cnt:
      raise Exception("Inconsistent number of records")
    function_arg_map["arguments"] = arg_map
    argument_mapping.append(function_arg_map)

  hdr = {}
  hdr["service"] = "127.0.0.1:3090"
  hdr["request_id"] = str(uuid.uuid1())
  hdr["input_sorted"] = False
  hdr["input_cnt"] = input_cnt
  hdr["time_zone"] = tz
  hdr["argument_mapping"] = argument_mapping

  ret = taqpy.Execute(json.dumps(hdr), **kwargs)
  ret_json = json.loads(ret[0])
  if "error_summary" not in ret_json.keys() or type(ret_json["error_summary"]) != type([]):
    ret_json["error_summary"] = []

  results = []
  results.append(ret_json)
  if "result_fields" in ret_json.keys() and len(ret) > 1:
    ret_flds = ret_json["result_fields"]
    data = {"ID" : ret[1]} # second member is ID array
    idx = 2
    for function, function_flds in ret_flds.items():
      for fld_name, fld_type in function_flds:
        data["{}.{}".format(function, fld_name)] = ret[idx]
        idx += 1
    results.append(data)

  requests = {}
  request_fields = {}
  request_functions = {}
  return results
