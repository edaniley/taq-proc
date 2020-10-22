import unittest
import os, signal
import subprocess
from sys import platform
import taqproc_testkit as tk
import taqpy


TRADE_DATE="20200801"

class TaqpyTest(unittest.TestCase):
  tickcalc = None
  trade_data = False

  @classmethod
  def setUpClass(cls):
    tk.AddSymbol("TEST")
    tk.AddSymbol("BAC")
    tk.AddSymbol("BRK A", SIP_Symbol="BRK.A", Round_Lot=1)
    tk.AddSymbol("XLK", Listed_Exchange="P", Tape="B")
    tk.AddSymbol("AMZN", Listed_Exchange="Q", Tape="C")
    tk.MakeSecmaster(TRADE_DATE)
    if platform  == "linux":
      cmd = "tick-calc -d /tmp/"
    else:
      cmd = R"tick-calc -d C:\tmp"
    cls.tickcalc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    print("Started tick-calc  pid:{}".format(cls.tickcalc.pid))

  @classmethod
  def tearDownClass(cls):
    os.kill(cls.tickcalc.pid, signal.SIGTERM)
    line = cls.tickcalc.stdout.readline()
    while line:
      # print(line.decode()[:-1])
      line = cls.tickcalc.stdout.readline()

  @classmethod
  def MakeTradeDataset(cls):
    if cls.trade_data:
        return
    cls.trade_data = True
    # additional arguments with default values
    # Exchange                       "N"
    # Sale_Condition                 "    "
    # Trade_Correction_Indicator     "00"
    # Source_of_Trade                "C"
    # Trade_Reporting_Facility       "N"
    # Trade_Through_Exempt_Indicator "0"
    tk.AddTrade('TEST', '09:30:00.654289', 100, 10.54)
    tk.AddTrade('TEST', '09:30:01.414499', 200, 10.96)
    tk.AddTrade('TEST', '09:30:02.20787', 300, 10.9)
    tk.AddTrade('TEST', '09:30:03.991933', 400, 10.85, Sale_Condition="Z")
    tk.AddTrade('TEST', '09:30:04.166208', 500, 10.82, Exchange="D")
    tk.AddTrade('TEST', '09:30:05.951611', 600, 10.34, Exchange="D")
    tk.AddTrade('TEST', '09:30:06.120973', 700, 10.99, Exchange="D")
    tk.AddTrade('TEST', '09:30:07.760147', 800, 10.64)
    tk.AddTrade('TEST', '09:30:08.104618', 900, 10.49, Exchange="D")
    tk.AddTrade('TEST', '09:30:09.106581', 100, 10.11)
    tk.AddTrade('TEST', '09:30:10.254205', 200, 10.46, Sale_Condition="Z")
    tk.AddTrade('TEST', '09:30:11.675591', 300, 10.33)
    tk.AddTrade('TEST', '09:30:12.408433', 400, 10.12)
    tk.AddTrade('TEST', '09:30:13.426745', 500, 10.72)
    tk.AddTrade('TEST', '09:30:14.960552', 600, 10.96)
    tk.AddTrade('TEST', '09:30:15.221821', 700, 10.93, Exchange="D")
    tk.AddTrade('TEST', '09:30:16.767575', 800, 10.15, Exchange="D")
    tk.AddTrade('TEST', '09:30:17.579457', 900, 10.91, Sale_Condition="Z")
    tk.AddTrade('TEST', '09:30:18.107083', 100, 10.84, Sale_Condition="Z")
    tk.AddTrade('TEST', '09:30:19.629419', 200, 10.03)
    tk.AddTrade('TEST', '09:30:20.100908', 300, 10.8)
    tk.AddTrade('TEST', '09:30:21.991891', 400, 10.75)
    tk.AddTrade('TEST', '09:30:22.118736', 500, 10.25)
    tk.AddTrade('TEST', '09:30:23.508291', 600, 10.16)
    tk.AddTrade('TEST', '09:30:24.548732', 700, 10.77)
    tk.AddTrade('TEST', '09:30:25.321846', 800, 10.6)
    tk.AddTrade('TEST', '09:30:26.471978', 900, 10.23)
    tk.AddTrade('TEST', '09:30:27.767195', 100, 10.86)
    tk.AddTrade('TEST', '09:30:28.15563', 200, 10.3)
    tk.AddTrade('TEST', '09:30:29.806084', 300, 10.77)
    tk.AddTrade('TEST', '09:30:30.305105', 400, 10.34)
    tk.AddTrade('TEST', '09:30:31.755329', 500, 10.88)
    tk.AddTrade('TEST', '09:30:32.421619', 600, 10.18)
    tk.AddTrade('TEST', '09:30:33.275699', 700, 10.61)
    tk.AddTrade('TEST', '09:30:34.572848', 10000, 10.93)
    tk.AddTrade('TEST', '09:30:35.95833', 9900, 20.51)
    tk.MakeTrades(TRADE_DATE)

  def test_Quote(self):
    tk.AddQuote("TEST", '09:30:01.123', 1.02, 1.12)
    tk.AddQuote("TEST", '09:30:03.123', 1.00, 1.10)
    tk.AddQuote("TEST", '09:31:01.123', 1.01, 1.11)
    tk.AddQuote("TEST", '10:30:01.123', 2.02, 12.12)
    tk.AddQuote("BAC", '09:30:01.123', 32.02, 33.12)
    tk.MakeQuotes(TRADE_DATE)
    tk.AddRequest(function_name="NBBO", alias="q1", Symbol="TEST", Timestamp = "2020-08-01T12:00:00.123456")
    tk.AddRequest(function_name="NBBO", alias="q1", Symbol="TEST", Timestamp = "2020-08-01T09:30:03")

    ret = tk.ExecuteRequests(TRADE_DATE)

    self.assertEqual(len(ret),2)
    self.assertEqual(len(ret[1]["ID"]), 2)

    results = ret[1]
    self.assertEqual(sorted(results.keys()),
      ['ID'
      ,'q1.BidPx', 'q1.BidQty', 'q1.OfferPx', 'q1.OfferQty', 'q1.Time'
      ])
    self.assertEqual(results["q1.BidPx"][0], 2.02)
    self.assertEqual(results["q1.BidPx"][1], 1.02)


  def disable_test_VWAP_FlavorsByTime(self):
    TaqpyTest.MakeTradeDataset()
    expected_results = {}
    # 1. unconstrained VWAP with end-time precisely matching eligible print
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Flavor=3)
    expected_results[1] = (19,	8900,	10.596854)
    # 2. unconstrained VWAP with end-time one usec erlier
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118735', Flavor=3)
    expected_results[2] = (18,	8400,	10.617500)
    # 3. unconstrained VWAP, exchange only
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Flavor=1)
    expected_results[3] = (13,	4700,	10.594255)
    # 4. unconstrained , off-exchange
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Flavor=2)
    expected_results[4] = (6,	4200,	10.599762)
    # 5. unconstrained , block
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:36', Flavor=4)
    expected_results[5] = (2,	19900,	15.695930)
    # 6. unconstrained, all (main trading session)
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:36', Flavor=5)
    expected_results[6] = (36,	36200,	13.392431)
    # 7. unconstrained, entire day of regular (VWAP-eligible) prints
    tk.AddRequest(function_name='VWAP', alias="v1", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '16:00:00', Flavor=3)
    expected_results[7] = (32,	34600,	13.510723)

    results = tk.ExecuteRequests(TRADE_DATE)

    self.assertEqual(len(results),1)
    self.assertEqual(len(results["VWAP"]), 2)
    arr_dict = results["VWAP"][1]
    ids = arr_dict["ID"]
    trd_cnt = arr_dict["TradeCnt"]
    trd_vol = arr_dict["TradeVolume"]
    vwap = arr_dict["VWAP"]
    for i in range(len(vwap)):
      expect = expected_results[ids[i]]
      self.assertEqual(expect[0], trd_cnt[i], 'id:{} TradeCnt mismatch'.format(ids[i]))
      self.assertEqual(expect[1], trd_vol[i], 'id:{} TradeVolume mismatch'.format(ids[i]))
      self.assertEqual(expect[2], vwap[i], 'id:{} VWAP mismatch'.format(ids[i]))

  def disable_test_VWAP_Markouts(self):
    TaqpyTest.MakeTradeDataset()
    tk.AddRequest(function_name='VWAP', alias="v2", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:14.960400',  Markouts="") # empty Markouts - should fail
    tk.AddRequest(function_name='VWAP', alias="v2", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:14.960400',  Markouts="-3t,3t,-11s,10s,100us,200us,300ms,0t,0h")
    tk.AddRequest(function_name='VWAP', alias="v2", Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:14.960400',  Markouts="-3t,3t") # inconsistent markouts length - should fail

    results = tk.ExecuteRequests(TRADE_DATE)

    self.assertEqual(len(results),1)
    self.assertEqual(len(results["VWAP"]), 2)

    exec_report = results["VWAP"][0]
    error_summary = exec_report["error_summary"]
    self.assertEqual(error_summary[0]["type"], "InvalidArgument")
    self.assertEqual(int(error_summary[0]["count"]), 2)
    self.assertEqual(int(exec_report["output_records"]), 1)

    arr_dict = results["VWAP"][1]
    # -3 ticks:     3	1200	10.422500
    self.assertEqual(arr_dict["TradeCnt_1"][0], 3)
    self.assertEqual(arr_dict["TradeVolume_1"][0], 1200)
    self.assertEqual(arr_dict["VWAP_1"][0], 10.422500)
    # 3 ticks:      3	2100	10.641429
    self.assertEqual(arr_dict["TradeCnt_2"][0], 3)
    self.assertEqual(arr_dict["TradeVolume_2"][0], 2100)
    self.assertEqual(arr_dict["VWAP_2"][0], 10.641429)
    # -11 seconds:  9	4800	10.578750
    self.assertEqual(arr_dict["TradeCnt_3"][0], 9)
    self.assertEqual(arr_dict["TradeVolume_3"][0], 4800)
    self.assertEqual(arr_dict["VWAP_3"][0], 10.578750)
    # 10 seconds:   9	4800	10.552708
    self.assertEqual(arr_dict["TradeCnt_4"][0], 9)
    self.assertEqual(arr_dict["TradeVolume_4"][0], 4800)
    self.assertEqual(arr_dict["VWAP_4"][0], 10.552708)
    # 100 microseconds:   0 0   0
    self.assertEqual(arr_dict["TradeCnt_5"][0], 0)
    self.assertEqual(arr_dict["TradeVolume_5"][0], 0)
    self.assertEqual(arr_dict["VWAP_5"][0], 0.0)
    # 200 microseconds:   1	600	10.960000
    self.assertEqual(arr_dict["TradeCnt_6"][0], 1)
    self.assertEqual(arr_dict["TradeVolume_6"][0], 600)
    self.assertEqual(arr_dict["VWAP_6"][0], 10.96)
    # 1500 milliseconds:  2	1300	10.943846
    self.assertEqual(arr_dict["TradeCnt_7"][0], 2)
    self.assertEqual(arr_dict["TradeVolume_7"][0], 1300)
    self.assertEqual(arr_dict["VWAP_7"][0], 10.943846)
    # 0 ticks:   0 0   0
    self.assertEqual(arr_dict["TradeCnt_8"][0], 0)
    self.assertEqual(arr_dict["TradeVolume_8"][0], 0)
    self.assertEqual(arr_dict["VWAP_8"][0], 0.0)
    # 0 hours:   0 0   0
    self.assertEqual(arr_dict["TradeCnt_9"][0], 0)
    self.assertEqual(arr_dict["TradeVolume_9"][0], 0)
    self.assertEqual(arr_dict["VWAP_9"][0], 0.0)

  def disable_test_VWAP_WithPriceConstrain(self):
    TaqpyTest.MakeTradeDataset()
    expected_results = {}
    # 1. limit VWAP with end-time precisely matching eligible print
    tk.AddRequest(function_name='VWAP', Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Side='B', LimitPx=10.58)
    expected_results[1] = (9,	3900,	10.284103)
    # 2. limit VWAP with end-time one usec erlier, should not include last print
    tk.AddRequest(function_name='VWAP', Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118735', Side='B', LimitPx=10.58)
    expected_results[2] = (8,	3400,	10.289118)
    # 3. limit VWAP
    tk.AddRequest(function_name='VWAP', Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00.000000',  EndTime = '16:00:00', Side='B', LimitPx=10.58)
    expected_results[3] = (14,	6600,	10.259848)

    results = tk.ExecuteRequests(TRADE_DATE)

    arr_dict = results["VWAP"][1]
    ids = arr_dict["ID"]
    trd_cnt = arr_dict["TradeCnt"]
    trd_vol = arr_dict["TradeVolume"]
    vwap = arr_dict["VWAP"]
    for i in range(len(vwap)):
      expect = expected_results[ids[i]]
      self.assertEqual(expect[0], trd_cnt[i], 'id:{} TradeCnt mismatch'.format(ids[i]))
      self.assertEqual(expect[1], trd_vol[i], 'id:{} TradeVolume mismatch'.format(ids[i]))
      self.assertEqual(expect[2], vwap[i], 'id:{} VWAP mismatch'.format(ids[i]))

  def disable_test_VWAP_ByPov(self):
    TaqpyTest.MakeTradeDataset()
    expected_results = {}
    tk.AddRequest(function_name='VWAP', Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00',  TargetVolume=250 , TargetPOV=0.02)
    expected_results[1] = (26,	12500,	10.560880)
    tk.AddRequest(function_name='VWAP', Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00',  TargetVolume=250 , TargetPOV=0.05)
    expected_results[2] = (12,	5400,	10.610000)
    # run out of prints -effectevely not volume-constrained
    # same as test_VWAP_FlavorsByTime No.7
    tk.AddRequest(function_name='VWAP', Symbol='TEST', Date = TRADE_DATE,
      StartTime = '09:30:00',  TargetVolume=10000 , TargetPOV=0.05)
    expected_results[3] = (32,	34600,	13.510723)

    results = tk.ExecuteRequests(TRADE_DATE)
    arr_dict = results["VWAP"][1]
    ids = arr_dict["ID"]
    trd_cnt = arr_dict["TradeCnt"]
    trd_vol = arr_dict["TradeVolume"]
    vwap = arr_dict["VWAP"]
    for i in range(len(vwap)):
      expect = expected_results[ids[i]]
      self.assertEqual(expect[0], trd_cnt[i], 'id:{} TradeCnt mismatch'.format(ids[i]))
      self.assertEqual(expect[1], trd_vol[i], 'id:{} TradeVolume mismatch'.format(ids[i]))
      self.assertEqual(expect[2], vwap[i], 'id:{} VWAP mismatch'.format(ids[i]))


if __name__ == "__main__":
  unittest.main()

