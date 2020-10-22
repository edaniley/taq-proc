import unittest
import taqproc_testkit as tk

def TestSimpleNBBO(trade_date):
    tk.AddRequest(function_name="NBBO", alias="q1", Symbol="TEST", Timestamp = "2020-08-01T12:00:00.123456")
    tk.AddRequest(function_name="NBBO", alias="q1", Symbol="TEST", Timestamp = "2020-08-01T09:30:03")

    ret = tk.ExecuteRequests(trade_date)

    assert len(ret) == 2
    assert len(ret[1]["ID"]) == 2

    results = ret[1]
    expected = ['ID' ,'q1.BidPx', 'q1.BidQty', 'q1.OfferPx', 'q1.OfferQty', 'q1.Time' ]
    assert sorted(results.keys()) == expected
    assert results["q1.BidPx"][0] ==  2.02
    assert results["q1.BidPx"][1] == 1.02

def exec(trade_date):
    TestSimpleNBBO(trade_date)