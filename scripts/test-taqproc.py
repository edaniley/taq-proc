import unittest
import os, signal
import subprocess

import taqproc_testkit as tk
import taqpy
from sys import platform

TRADE_DATE="20200801"

class TaqptTest(unittest.TestCase):
  tickcalc = None

  @classmethod
  def setUpClass(cls):
    tk.AddSymbol("TEST")
    tk.AddSymbol("BAC")
    tk.AddSymbol("BRK A", SIP_Symbol="BRK.A", Round_Lot=1)
    tk.AddSymbol("XLK", Listed_Exchange="P", Tape="B")
    tk.AddSymbol("AMZN", Listed_Exchange="Q", Tape="C")
    tk.MakeSecmaster(TRADE_DATE)
    if platform  == "linux":
      cmd = "tick-calc -d /tmp/ -v on"
    else:
      cmd = R"tick-calc -d C:\Users\ed.danileyko.LAPTOP-ED\Work\taq-proc\data -v on"
    cls.tickcalc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    print("Started tick-calc  pid:{}".format(cls.tickcalc.pid))

  @classmethod
  def tearDownClass(cls):
    os.kill(cls.tickcalc.pid, signal.SIGTERM)
    line = cls.tickcalc.stdout.readline()
    while line:
      # print(line.decode()[:-1])
      line = cls.tickcalc.stdout.readline()


  def test_Quote(self):
    tk.AddQuote("TEST", '09:30:03.123', 1.00, 1.10)
    tk.AddQuote("TEST", '09:31:01.123', 1.01, 1.11)
    tk.AddQuote("TEST", '09:30:01.123', 1.02, 1.12)
    tk.AddQuote("TEST", '10:30:01.123', 2.02, 12.12)
    tk.AddQuote("BAC", '09:30:01.123', 32.02, 33.12)
    tk.MakeQuotes(TRADE_DATE)

    tk.AddRequest(function_name="NBBO)", Symbol="TEST", Timestamp = "2020-08-01T12:00:00.123456")
    tk.AddRequest(function_name="NBBO", Symbol="TEST", Timestamp = "2020-08-01T10:00:00.123456")

    results = tk.ExecuteRequests(TRADE_DATE)

    self.assertEqual(len(results),1)
    self.assertEqual(len(results["NBBO"]), 2)

    arr_dict = results["Quote"][1]
    self.assertEqual(sorted(arr_dict.keys()),
            ['BestBidPx', 'BestBidQty', 'BestOfferPx', 'BestOfferQty', 'ID', 'Timestamp'])
    self.assertEqual(arr_dict["BestBidPx"][0], 2.02)
    self.assertEqual(arr_dict["BestBidPx"][1], 1.01)

  def test_VWAP_ByTime(self):
    # additional arguments with default values
    # Exchange                       "N"
    # Sale_Condition                 "    "
    # Trade_Correction_Indicator     "00"
    # Source_of_Trade                "C"
    # Trade_Reporting_Facility       "N"
    # Trade_Through_Exempt_Indicator "0"

    tk.AddTrade('TEST',  '09:30:01.123123', 134, 10.12)
    tk.AddTrade('TEST',  '09:30:01.600000', 500, 10.15)
    tk.AddTrade('TEST',  '09:30:03.123000', 100, 10.1)
    tk.AddTrade('TEST',  '09:31:01.123000', 200, 10.15, Sale_Condition="AZ")
    tk.AddTrade('TEST',  '09:31:01.123000', 200, 11.15, Exchange="D")
    tk.AddTrade('TEST',  '09:31:05.000000', 300, 10.15, Exchange="D")
    tk.AddTrade('TEST',  '09:31:05.000000', 300, 10.15, Exchange="D")
    tk.AddTrade('TEST',  '09:33:04.999998', 200, 10.15)
    tk.AddTrade('TEST',  '09:33:04.999999', 200, 10.15, Exchange="D")
    tk.AddTrade('TEST',  '09:33:04.500000', 200, 10.15)
    tk.AddTrade('TEST',  '10:30:00.000000', 200, 10.1, Sale_Condition="AZ")
    tk.AddTrade('TEST',  '10:30:01.000000', 200, 10.1)
    tk.MakeTrades(TRADE_DATE)

    tk.AddRequest(function_name="VWAP", Symbol="TEST", Date = TRADE_DATE, StartTime = "09:30:00.000000", EndTime = "16:00:00")
    tk.AddRequest(function_name="VWAP", Symbol="TEST", Date = TRADE_DATE, StartTime = "09:30:00.000000", EndTime = "10:00:00")
    tk.AddRequest(function_name="VWAP", Symbol="TEST", Date = TRADE_DATE, StartTime = "12:00:00.123456", EndTime = "16:00:00")

    results = tk.ExecuteRequests(TRADE_DATE)

    self.assertEqual(len(results),1)
    self.assertEqual(len(results["VWAP"]), 2)

    arr_dict = results["VWAP"][1]
    ids = arr_dict["ID"]
    trd_cnt = arr_dict["TradeCnt"]
    trd_vol = arr_dict["TradeVolume"]
    vwap = arr_dict["VWAP"]
    self.assertEqual(sorted(arr_dict.keys()), ['ID', 'TradeCnt', 'TradeVolume', 'VWAP'])
    self.assertEqual(ids[2], 3)
    print ("trd_cnt: {}".format(trd_cnt))
    print ("trd_vol: {}".format(trd_vol))
    print ("vwap: {}".format(vwap))
    self.assertEqual(vwap[0], 10.2275)
    self.assertEqual(vwap[1], 10.2395)
    self.assertEqual(vwap[2], 0)

if __name__ == "__main__":
  unittest.main()

