

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
      cmd = "tick-calc -d /home/edaniley/Work/taq-proc/data -v on"
    else:
      cmd = R"tick-calc -d C:\Users\ed.danileyko.LAPTOP-ED\Work\taq-proc\data -v on"
    cls.tickcalc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    print("Started tick-calc  pid:{}".format(cls.tickcalc.pid))

  @classmethod
  def tearDownClass(cls):
    os.kill(cls.tickcalc.pid, signal.SIGTERM)
    line = cls.tickcalc.stdout.readline()
    while line:
      print(line.decode()[:-1])
      line = cls.tickcalc.stdout.readline()


  def test_Quote(self):
    tk.AddQuote("TEST", '09:30:03.123', 1.00, 1.10)
    tk.AddQuote("TEST", '09:31:01.123', 1.01, 1.11)
    tk.AddQuote("TEST", '09:30:01.123', 1.02, 1.12)
    tk.AddQuote("TEST", '10:30:01.123', 2.02, 12.12)
    tk.AddQuote("BAC", '09:30:01.123', 32.02, 33.12)
    tk.MakeQuotes(TRADE_DATE)

    tk.AddRequest(function_name="Quote", Symbol="TEST", Timestamp = "2020-08-01T12:00:00.123456")
    tk.AddRequest(function_name="Quote", Symbol="TEST", Timestamp = "2020-08-01T10:00:00.123456")

    results = tk.ExecuteRequests(TRADE_DATE)

    self.assertEqual(len(results),1)
    self.assertEqual(len(results["Quote"]),2)
    df = results["Quote"][1]
    self.assertEqual(len(df),2)
    self.assertEqual(df.loc[0]["BestBidPx"], 2.02)
    self.assertEqual(df.loc[1]["BestBidPx"], 1.01)

  def test_Trade(self):
    tk.AddTrade("TEST", '09:30:03.123', 100, 1.10)
    tk.AddTrade("TEST", '09:31:01.123', 2000, 1.11)
    tk.AddTrade("TEST", '09:30:01.123', 134, 1.12)
    tk.AddTrade("TEST", '10:30:01.50', 200, 1.12)
    tk.AddTrade("TEST", '09:30:01.60', 200, 1.15)
    tk.MakeTrades(TRADE_DATE)

    tk.AddRequest(function_name="VWAP", Symbol="SPY", Date = "20200331", StartTime = "12:00:00.123456",
                  EndTime = "16:00:00"
                  )
    tk.AddRequest(function_name="VWAP", Symbol="SPY", Date = "20200331", StartTime = "12:00:00.123456",
                  EndTime = "14:00:00"
                  )
    tk.AddRequest(function_name="VWAP", Symbol="BAC", Date = "20200331", StartTime = "12:00:00.123456",
                  EndTime = "16:00:00"
                  )
    results = tk.ExecuteRequests(TRADE_DATE)


if __name__ == "__main__":
  unittest.main()

