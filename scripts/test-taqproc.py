

import unittest
import os, signal
import subprocess
import taqproc_testkit as tk
import taqpy

class TaqptTest(unittest.TestCase):
  tickcalc = None

  @classmethod
  def setUpClass(cls):
    tk.AddSymbol("TEST")
    tk.AddSymbol("BAC")
    tk.AddSymbol("BRK A", SIP_Symbol="BRK.A", Round_Lot=1)
    tk.AddSymbol("XLK", Listed_Exchange="P", Tape="B")
    tk.AddSymbol("AMZN", Listed_Exchange="Q", Tape="C")
    tk.MakeSecmaster('20200801')
    cls.tickcalc = subprocess.Popen("tick-calc -d /home/edaniley/Work/taq-proc/data -v on", shell=True, stdout=subprocess.PIPE)
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
    tk.MakeQuotes('20200801')

    tk.AddRequest(function_name="Quote", Symbol="TEST", Timestamp = "2020-08-01T12:00:00.123456")
    tk.AddRequest(function_name="Quote", Symbol="TEST", Timestamp = "2020-08-01T10:00:00.123456")

    results = tk.ExecuteRequests("20200801")

    self.assertEqual(len(results),1)
    self.assertEqual(len(results["Quote"]),2)
    df = results["Quote"][1]
    self.assertEqual(len(df),2)
    self.assertEqual(df.loc[0]["BestBidPx"], 2.02)
    self.assertEqual(df.loc[1]["BestBidPx"], 1.01)


if __name__ == "__main__":
  unittest.main()

