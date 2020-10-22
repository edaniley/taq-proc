import unittest
import taqproc_testkit as tk

def test_VWAP_FlavorsByTime(trade_date):
    expected_results = {}
    # 1. unconstrained VWAP with end-time precisely matching eligible print
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Flavor=3)
    expected_results[1] = (19,	8900,	10.596854)
    # 2. unconstrained VWAP with end-time one usec erlier
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118735', Flavor=3)
    expected_results[2] = (18,	8400,	10.617500)
    # 3. unconstrained VWAP, exchange only
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Flavor=1)
    expected_results[3] = (13,	4700,	10.594255)
    # 4. unconstrained , off-exchange
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Flavor=2)
    expected_results[4] = (6,	4200,	10.599762)
    # 5. unconstrained , block
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:36', Flavor=4)
    expected_results[5] = (2,	19900,	15.695930)
    # 6. unconstrained, all (main trading session)
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:36', Flavor=5)
    expected_results[6] = (36,	36200,	13.392431)
    # 7. unconstrained, entire day of regular (VWAP-eligible) prints
    tk.AddRequest(alias="v1", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '16:00:00', Flavor=3)
    expected_results[7] = (32,	34600, 13.510723)

    results = tk.ExecuteRequests(trade_date)

    assert len(results) == 2
    assert len(results[1]["ID"]) == 7
    ids = [id for id in results[1]["ID"]]
    trd_cnt = results[1]["v1.TradeCnt"]
    trd_vol = results[1]["v1.TradeVolume"]
    vwap = results[1]["v1.VWAP"]
    for i in range(len(ids)):
      exp_trd_cnt, exp_trd_vol, exp_vwap = expected_results[ids[i]]
      assert exp_trd_cnt == trd_cnt[i], 'id:{} TradeCnt mismatch'.format(ids[i])
      assert exp_trd_vol == trd_vol[i], 'id:{} TradeVolume mismatch'.format(ids[i])
      assert exp_vwap == vwap[i], 'id:{} VWAP mismatch'.format(ids[i])

def test_VWAP_Markouts(trade_date):
    tk.AddRequest(alias="v2", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:14.960400',  Markouts="") # empty Markouts - should fail
    tk.AddRequest(alias="v2", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:14.960400',  Markouts="-3t,3t,-11s,10s,100us,200us,300ms,0t,0h")
    tk.AddRequest(alias="v2", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:14.960400',  Markouts="-3t,3t") # inconsistent markouts length - should fail

    results = tk.ExecuteRequests(trade_date)

    assert len(results) == 2
    assert len(results[1]["ID"]) == 1

    exec_report = results[0]
    error_summary = exec_report["error_summary"]
    assert error_summary[0]["type"] == "InvalidArgument"
    assert int(error_summary[0]["count"]) == 2
    assert int(exec_report["output_records"]) == 1

    arr_dict = results[1]
    # -3 ticks:     3	1200	10.422500
    assert arr_dict["v2.TradeCnt_1"][0] == 3
    assert arr_dict["v2.TradeVolume_1"][0] == 1200
    assert arr_dict["v2.VWAP_1"][0] == 10.422500
    # 3 ticks:      3	2100	10.641429
    assert arr_dict["v2.TradeCnt_2"][0] == 3
    assert arr_dict["v2.TradeVolume_2"][0] == 2100
    assert arr_dict["v2.VWAP_2"][0] == 10.641429
    # -11 seconds:  9	4800	10.578750
    assert arr_dict["v2.TradeCnt_3"][0] == 9
    assert arr_dict["v2.TradeVolume_3"][0] == 4800
    assert arr_dict["v2.VWAP_3"][0] == 10.578750
    # 10 seconds:   9	4800	10.552708
    assert arr_dict["v2.TradeCnt_4"][0] == 9
    assert arr_dict["v2.TradeVolume_4"][0] == 4800
    assert arr_dict["v2.VWAP_4"][0] == 10.552708
    # 100 microseconds:   0 0   0
    assert arr_dict["v2.TradeCnt_5"][0] == 0
    assert arr_dict["v2.TradeVolume_5"][0] == 0
    assert arr_dict["v2.VWAP_5"][0] == 0.0
    # 200 microseconds:   1	600	10.960000
    assert arr_dict["v2.TradeCnt_6"][0] == 1
    assert arr_dict["v2.TradeVolume_6"][0] == 600
    assert arr_dict["v2.VWAP_6"][0] == 10.96
    # 1500 milliseconds:  2	1300	10.943846
    assert arr_dict["v2.TradeCnt_7"][0] == 2
    assert arr_dict["v2.TradeVolume_7"][0] == 1300
    assert arr_dict["v2.VWAP_7"][0] == 10.943846
    # 0 ticks:   0 0   0
    assert arr_dict["v2.TradeCnt_8"][0] == 0
    assert arr_dict["v2.TradeVolume_8"][0] == 0
    assert arr_dict["v2.VWAP_8"][0] == 0.0
    # 0 hours:   0 0   0
    assert arr_dict["v2.TradeCnt_9"][0] == 0
    assert arr_dict["v2.TradeVolume_9"][0] == 0
    assert arr_dict["v2.VWAP_9"][0] == 0.0

def test_VWAP_WithPriceConstrain(trade_date):
    expected_results = {}
    # 1. limit VWAP with end-time precisely matching eligible print
    tk.AddRequest(alias="v3", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118736', Side='B', LimitPx=10.58)
    expected_results[1] = (9,	3900,	10.284103)
    # 2. limit VWAP with end-time one usec erlier, should not include last print
    tk.AddRequest(alias="v3", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '09:30:22.118735', Side='B', LimitPx=10.58)
    expected_results[2] = (8,	3400,	10.289118)
    # 3. limit VWAP
    tk.AddRequest(alias="v3", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00.000000',  EndTime = '16:00:00', Side='B', LimitPx=10.58)
    expected_results[3] = (14,	6600,	10.259848)

    results = tk.ExecuteRequests(trade_date)
    arr_dict = results[1]

    assert len(results) == 2
    assert len(arr_dict["ID"]) == 3

    ids = arr_dict["ID"]
    trd_cnt = arr_dict["v3.TradeCnt"]
    trd_vol = arr_dict["v3.TradeVolume"]
    vwap = arr_dict["v3.VWAP"]
    for i in range(len(ids)):
      exp_trd_cnt, exp_trd_vol, exp_vwap = expected_results[ids[i]]
      assert exp_trd_cnt == trd_cnt[i], 'id:{} TradeCnt mismatch'.format(ids[i])
      assert exp_trd_vol == trd_vol[i], 'id:{} TradeVolume mismatch'.format(ids[i])
      assert exp_vwap == vwap[i], 'id:{} VWAP mismatch'.format(ids[i])

def test_VWAP_ByPov(trade_date):
    expected_results = {}
    tk.AddRequest(alias="v4", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00',  TargetVolume=250 , TargetPOV=0.02)
    expected_results[1] = (26,	12500,	10.560880)
    tk.AddRequest(alias="v4", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00',  TargetVolume=250 , TargetPOV=0.05)
    expected_results[2] = (12,	5400,	10.610000)
    # run out of prints -effectevely not volume-constrained
    # same as test_VWAP_FlavorsByTime No.7
    tk.AddRequest(alias="v4", function_name='VWAP', Symbol='TEST', Date = trade_date,
      StartTime = '09:30:00',  TargetVolume=10000 , TargetPOV=0.05)
    expected_results[3] = (32,	34600,	13.510723)

    results = tk.ExecuteRequests(trade_date)
    arr_dict = results[1]

    assert len(results) == 2
    assert len(arr_dict["ID"]) == 3

    ids = arr_dict["ID"]
    trd_cnt = arr_dict["v4.TradeCnt"]
    trd_vol = arr_dict["v4.TradeVolume"]
    vwap = arr_dict["v4.VWAP"]
    for i in range(len(ids)):
      exp_trd_cnt, exp_trd_vol, exp_vwap = expected_results[ids[i]]
      assert exp_trd_cnt == trd_cnt[i], 'id:{} TradeCnt mismatch'.format(ids[i])
      assert exp_trd_vol == trd_vol[i], 'id:{} TradeVolume mismatch'.format(ids[i])
      assert exp_vwap == vwap[i], 'id:{} VWAP mismatch'.format(ids[i])


def exec(trade_date):
    test_VWAP_ByPov(trade_date)
    test_VWAP_WithPriceConstrain(trade_date)
    test_VWAP_FlavorsByTime(trade_date)
    test_VWAP_Markouts(trade_date)