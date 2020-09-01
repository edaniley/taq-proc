#include <tuple>
#include <algorithm>
#include <iterator>
#include <cmath>

#include <boost/algorithm/string.hpp>
#include "taq-proc.h"
#include "tick-calc.h"
#include "tick-func-vwap.h"
#include "double.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

static bool IsArgumentPresent(const vector<int> & arg_map, int f) {
  return f < (int)arg_map.size() && arg_map[f] != -1;
}

static char DecodeSide(const string& str) {
  if (str.size() > 0) {
    if (str[0] == 'B' || str[0] == 'b')
      return 'B';
    else if (str[0] == 'S' || str[0] == 's')
      return 'S';
  }
  return '\0';
}

static VwapFlavor DecodeVwapFlavor(const string& str) {
  if (str.empty() == false) {
    int val = stoi(str);
    return val > 0 && val < (int)VwapFlavor::MaxVal ? (VwapFlavor)val : VwapFlavor::Unknown;
  }
  return VwapFlavor::Regular;
}

class VwapFunction : public  FunctionDefinition {
public:
  enum ArgumentNo { Symbol, Date, StartTime, Side, LimitPx, Flavor, EndTime, TargetVolume, TargetPOV, Ticks };
  VwapFunction(const string& name, const vector<string>& argument_names, const vector<string>& output_fields) :
    FunctionDefinition(name, argument_names, output_fields) {}
  void ValidateArgumentList(const vector<int>& argument_mapping) const override {
    bool valid = IsArgumentPresent(argument_mapping, Symbol)
              && IsArgumentPresent(argument_mapping, Date)
              && IsArgumentPresent(argument_mapping, StartTime);
    valid = valid && (
           IsArgumentPresent(argument_mapping, EndTime)
        || (IsArgumentPresent(argument_mapping, TargetVolume) && IsArgumentPresent(argument_mapping, TargetPOV))
        || IsArgumentPresent(argument_mapping, Ticks)
      );
    if (!valid) {
      throw invalid_argument("Invalid argument list");
    }
  }
};

static const bool registered = RegisterFunctionDefinition(make_unique<VwapFunction>("VWAP",
    vector<string> {"Symbol", "Date", "StartTime", "Side", "LimitPx", "Flavor",
                    "EndTime", "TargetVolume", "TargetPOV", "Ticks"},
    vector<string> {"ID", "TradeCnt", "TradeVolume", "VWAP"})
    );

static bool IsEligible(const Trade& trd, char side, Double price) {
  return trd.attr.ve && (
    price.Empty()
    || (side == 'B' && price.LessOrEqual(trd.price))
    || (side == 'S' && price.GreaterOrEqual(trd.price))
    );
}

static bool IsNormal(const Trade& trd) {
  return 0 == memcmp(trd.cond, "    ", 4) || 0 == memcmp(trd.cond, "@   ", 4);
}

static bool IsExchange(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && (char)trd.attr.exch != 'D' && IsNormal(trd);
}

static bool IsTRF(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && (char)trd.attr.exch == 'D' && IsNormal(trd);
}

static bool IsRegular(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && IsNormal(trd);
}

void VwapExecutionUnit::Execute() {
  auto& secmaster_mgr = SecurityMasterManager();
  auto& trade_mgr = TradeRecordsetManager();
  const SecMaster* secmaster = nullptr;
  const SymbolRecordset<Trade>* symbol_recordset = nullptr;
  try {
    secmaster = &secmaster_mgr.Load(date);
    const Security& security = secmaster->FindBySymbol(symbol);
    symbol_recordset = &trade_mgr.LoadSymbolRecordset(date, security.symb);
  }
  catch (...) {
    Error(ErrorType::DataNotFound, (int)input_records.size());
    return;
  }
  if (false == input_sorted) {
    sort(input_records.begin(), input_records.end(), [](const auto& lh, const auto& rh) {
      return lh.start_time < rh.start_time;
    });
  }
  const Time taq_time_adjustment = adjust_time ? UtcToTaq(date) : ZeroTime();
  auto& trades = symbol_recordset->records;
  auto it = trades.begin();
  for (auto rec : input_records) {
    auto validator = rec.flavor == (uint8_t)VwapFlavor::Regular ? IsRegular
                   : rec.flavor == (uint8_t)VwapFlavor::Exchange ? IsExchange
                   : rec.flavor == (uint8_t)VwapFlavor::TRF ? IsTRF : IsEligible;
    const Time requested_time = rec.start_time + taq_time_adjustment;
    it = trades.lower_bound(it, trades.end(), requested_time);
    uint64_t trade_count = 0, trade_volume = 0;
    double vwap = .0;
    if (rec.end_time.type == VwapDurationType::ByTime) {
      const Time end_time = requested_time +  (rec.end_time.duration.end_time - rec.start_time);
      for (auto trd = it; trd < trades.end() && trd->time < end_time; ++ trd) {
        if (validator(*trd, rec.side, rec.limit_price)) {
          trade_count ++;
          trade_volume += trd->qty;
          vwap += trd->price * trd->qty;
        }
      }
    }
    else if (rec.end_time.type == VwapDurationType::ByPov) {
      int64_t target_vol = (int64_t)ceil((double)rec.end_time.duration.pov.first / rec.end_time.duration.pov.second);
      for (auto trd = it; trd < trades.end() && target_vol > 0; ++ trd) {
        if (validator(*trd, rec.side, rec.limit_price)) {
          trade_count ++;
          trade_volume += trd->qty;
          vwap += trd->price * trd->qty;
          target_vol -= trd->qty;
        }
      }
    }
    else if (rec.end_time.duration.ticks > 0 ) {
      int target_cnt = rec.end_time.duration.ticks;
      for (auto trd = it; trd < trades.end() && target_cnt > 0; ++ trd) {
        if (validator(*trd, rec.side, rec.limit_price)) {
          trade_count ++;
          trade_volume += trd->qty;
          vwap += trd->price * trd->qty;
          target_cnt --;
        }
      }
    }
    else if (rec.end_time.duration.ticks < 0) {
      int target_cnt = abs(rec.end_time.duration.ticks);
      for (auto trd = it; target_cnt > 0; -- trd) {
        if (validator(*trd, rec.side, rec.limit_price)) {
          trade_count ++;
          trade_volume += trd->qty;
          vwap += trd->price * trd->qty;
          target_cnt--;
        }
        if (trd == trades.begin())
          break;
      }
    }
    ostringstream ss;
    if (trade_volume) {
      vwap /= trade_volume;
    }
    ss << rec.id << '|' << trade_count << '|' << trade_volume << '|' << vwap << endl;
    output_records.emplace_back(rec.id, ss.str());
  }
  trade_mgr.UnloadSymbolRecordset(date, symbol);
  secmaster_mgr.Release(*secmaster);
}

VwapExecutionPlan::VwapExecutionPlan( const FunctionDefinition& function,
                                      const Request& request,
                                      const vector<int>& argument_mapping)
  : ExecutionPlan(function, request, argument_mapping),
    maybe_by_time(IsArgumentPresent(argument_mapping, VwapFunction::EndTime)),
    maybe_by_pov(IsArgumentPresent(argument_mapping, VwapFunction::TargetVolume)
              && IsArgumentPresent(argument_mapping, VwapFunction::TargetPOV)),
    maybe_by_ticks(IsArgumentPresent(argument_mapping, VwapFunction::Ticks)),
    duration_type(maybe_by_time && false == maybe_by_pov && false == maybe_by_ticks ? VwapDurationType::ByTime
      : false == maybe_by_time && maybe_by_pov && false == maybe_by_ticks ? VwapDurationType::ByPov
      : false == maybe_by_time && false == maybe_by_pov && maybe_by_ticks ? VwapDurationType::ByTicks : VwapDurationType::RecordSpecific) {
}


void VwapExecutionPlan::Input(InputRecord & input_record) {
  try {
    const string& symbol = input_record.values[argument_mapping[VwapFunction::Symbol]];
    const Date date = MkTaqDate(input_record.values[argument_mapping[VwapFunction::Date]]);
    InputRecordRange& input_range = input_record_ranges[make_pair(symbol, date)];
    const Time start_time = MkTime(input_record.values[argument_mapping[VwapFunction::StartTime]]);
    const char side = IsArgumentPresent(argument_mapping, VwapFunction::Side)
      ? DecodeSide(input_record.values[argument_mapping[VwapFunction::Side]]) : '\0';
    const Double limit_price = side && IsArgumentPresent(argument_mapping, VwapFunction::LimitPx)
      ? Double(input_record.values[argument_mapping[VwapFunction::LimitPx]]) : Double();
    const VwapFlavor flavor = IsArgumentPresent(argument_mapping, VwapFunction::Flavor)
      ? DecodeVwapFlavor(input_record.values[argument_mapping[VwapFunction::Flavor]]) : VwapFlavor::Regular;

    if (flavor == VwapFlavor::Unknown) {
      throw Exception(ErrorType::InvalidArgument);
    }

    if (duration_type == VwapDurationType::ByTime) {
      const Time end_time = MkTime(input_record.values[argument_mapping[VwapFunction::EndTime]]);
      input_range.emplace_back(input_record.id, start_time, end_time, side, limit_price, flavor);
    }
    else if (duration_type == VwapDurationType::ByPov) {
      const uint64_t vol = stoul(input_record.values[argument_mapping[VwapFunction::TargetVolume]]);
      const double pov = stod(input_record.values[argument_mapping[VwapFunction::TargetPOV]]);
      input_range.emplace_back(input_record.id, start_time, make_pair(vol, pov), side, limit_price, flavor);
    }
    else if (duration_type == VwapDurationType::ByTicks) {
      const int ticks = stoul(input_record.values[argument_mapping[VwapFunction::Ticks]]);
      input_range.emplace_back(input_record.id, start_time, ticks, side, limit_price, flavor);
    }
    else { //VwapDurationType::RecordSpecific
      VwapDurationType type = VwapDurationType::Unknown;
      if (maybe_by_time) {
        const string & str = input_record.values[argument_mapping[VwapFunction::EndTime]];
        if (str.empty() == false) {
          const Time end_time = MkTime(str);
          input_range.emplace_back(input_record.id, start_time, end_time, side, limit_price, flavor);
          type = VwapDurationType::ByTime;
        }
      }
      if (maybe_by_pov && type == VwapDurationType::Unknown) {
        const string & str1 = input_record.values[argument_mapping[VwapFunction::TargetVolume]];
        const string & str2 = input_record.values[argument_mapping[VwapFunction::TargetPOV]];
        if (str1.empty() == false && str2.empty() == false) {
          const uint64_t vol = stoul(str1);
          const double pov = stod(str2);
          input_range.emplace_back(input_record.id, start_time, make_pair(vol, pov), side, limit_price, flavor);
          type = VwapDurationType::ByPov;
        }
      }
      if (maybe_by_ticks && type == VwapDurationType::Unknown) {
        const string& str = input_record.values[argument_mapping[VwapFunction::Ticks]];
        if (str.empty() == false) {
          const int ticks = stoul(str);
          input_range.emplace_back(input_record.id, start_time, ticks, side, limit_price, flavor);
          type = VwapDurationType::ByTicks;
        }
      }
      if (type == VwapDurationType::Unknown) {
        throw Exception(ErrorType::InvalidArgument);
      }
    }
  }
  catch (const Exception& Ex) {
    Error(Ex.errtype());
  }
  catch (...) {
    Error(ErrorType::InvalidArgument);
  }
}

void VwapExecutionPlan::Execute() {
  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto& range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  sort(slices.begin(), slices.end(), [](const InputRecordSlice& left, const InputRecordSlice& right) {
    return (get<2>(left)->size() > get<2>(right)->size());
    });
  for (auto& slice : slices) {
    shared_ptr<ExecutionUnit> job = make_shared<VwapExecutionUnit>(
      get<0>(slice), get<1>(slice), request.input_sorted, request.tz_name == "UTC", *get<2>(slice)
      );
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

}
