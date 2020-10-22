import pytest
import os, signal
import subprocess
from sys import platform
import taqproc_testkit as tk
import taqpy

import test_nbbo
import test_vwap


TRADE_DATE="20200801"
tickcalc = None

def setup():
    global tickcalc
    MakeSecmasterDataset()
    MakeTradeDataset()
    MakeQuoteDataset()
    if platform  == "linux":
      cmd = "tick-calc -d /tmp/"
    else:
      cmd = R"tick-calc -d C:\tmp"
    tickcalc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    print("Started tick-calc  pid:{}".format(tickcalc.pid))


def teardown():
    global tickcalc
    if tickcalc:
        os.kill(tickcalc.pid, signal.SIGTERM)
        line = tickcalc.stdout.readline()
        while line:
            print(line.decode()[:-1])
            line = tickcalc.stdout.readline()

def MakeSecmasterDataset():
    tk.AddSymbol("TEST")
    tk.AddSymbol("BAC")
    tk.AddSymbol("BRK A", SIP_Symbol="BRK.A", Round_Lot=1)
    tk.AddSymbol("XLK", Listed_Exchange="P", Tape="B")
    tk.AddSymbol("AMZN", Listed_Exchange="Q", Tape="C")
    tk.MakeSecmaster(TRADE_DATE)


def MakeTradeDataset():
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

def MakeQuoteDataset():
    tk.AddQuote("TEST", '09:30:01.123', 1.02, 1.12)
    tk.AddQuote("TEST", '09:30:03.123', 1.00, 1.10)
    tk.AddQuote("TEST", '09:31:01.123', 1.01, 1.11)
    tk.AddQuote("TEST", '10:30:01.123', 2.02, 12.12)
    tk.AddQuote("BAC", '09:30:01.123', 32.02, 33.12)
    tk.MakeQuotes(TRADE_DATE)

def test_all():
    #test_nbbo.exec(TRADE_DATE)
    test_vwap.exec(TRADE_DATE)

if __name__ == "__main__":
    setup()
    test_all()
    teardown()
