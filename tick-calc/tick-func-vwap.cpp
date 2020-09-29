
#include <tuple>
#include <algorithm>

#include "taq-proc.h"
#include "tick-calc.h"
#include "tick-func-vwap.h"

namespace tick_calc {
namespace VWAP {

static char DecodeSide(const string_view& str) {
  if (str.size() > 0) {
    if (str[0] == 'B' || str[0] == 'b')
      return 'B';
    else if (str[0] == 'S' || str[0] == 's')
      return 'S';
  }
  return '\0';
}

static Flavor DecodeFlavor(const string_view& str) {
  if (str.empty() == false) {
    int val = TextToNumeric<int>(str);
    return val > 0 && val < (int)Flavor::MaxVal ? (Flavor)val : Flavor::Unknown;
  }
  return Flavor::Regular;
}

class VwapFunction : public  FunctionDefinition {
public:
  enum ArgumentNo { Symbol, Date, StartTime, Side, LimitPx, Flavor, EndTime, TargetVolume, TargetPOV, Ticks, Markouts };
  VwapFunction(const string& name, const vector<string>& argument_names, const vector<string>& output_fields) :
    FunctionDefinition(name, argument_names, output_fields) {}
  void ValidateArgumentList(const vector<int>& argument_mapping) const override {
    bool valid = IsArgumentPresent(argument_mapping, Symbol)
              && IsArgumentPresent(argument_mapping, Date)
              && IsArgumentPresent(argument_mapping, StartTime);
    valid = valid && (
           IsArgumentPresent(argument_mapping, EndTime)
        || IsArgumentPresent(argument_mapping, Ticks)
        || IsArgumentPresent(argument_mapping, Markouts)
        || (IsArgumentPresent(argument_mapping, TargetVolume) && IsArgumentPresent(argument_mapping, TargetPOV))
      );
    if (!valid) {
      throw invalid_argument("Invalid argument list");
    }
  }
};

static const bool registered = RegisterFunctionDefinition(make_unique<VwapFunction>("VWAP",
    vector<string> {"Symbol", "Date", "StartTime", "Side", "LimitPx", "Flavor",
                    "EndTime", "TargetVolume", "TargetPOV", "Ticks", "Markouts"},
    vector<string> {"ID", "TradeCnt", "TradeVolume", "VWAP"})
    );


void ExecutionUnit::Execute() {
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
  auto& trades = symbol_recordset->records;
  auto it = trades.begin();
  for (auto rec : input_records) {
    const Time start_time = rec.start_time + taq_time_adjustment;
    it = trades.lower_bound(it, trades.end(), start_time);
    vector<Result> results(rec.durations.size());
    for (size_t i = 0; i < rec.durations.size(); i ++) {
      Calculator calculator(trades, rec.start_time, rec.durations[i], rec.side, rec.limit_price, rec.flavor);
      results[i] = calculator.Calculate(it);
    }
    ostringstream ss;
    ss << rec.id;
    for (const Result &result : results) {
      char buf[64];
      #ifdef __unix__
      sprintf(buf, "|%lu|%lu|%f", result.trade_count, result.trade_volume, result.vwap);
      #else
      sprintf_s(buf, sizeof(buf), "|%llu|%llu|%f", result.trade_count, result.trade_volume, result.vwap);
      #endif
      ss << buf;
    }
    ss << endl;
    output_records.emplace_back(rec.id, ss.str());
  }
  trade_mgr.UnloadSymbolRecordset(date, symbol);
  secmaster_mgr.Release(*secmaster);
}

ExecutionPlan::ExecutionPlan( const FunctionDefinition& function,
                                      const Request& request,
                                      const vector<int>& argument_mapping)
  : tick_calc::ExecutionPlan(function, request, argument_mapping),
    maybe_by_time(IsArgumentPresent(argument_mapping, VwapFunction::EndTime)),
    maybe_by_pov(IsArgumentPresent(argument_mapping, VwapFunction::TargetVolume)
              && IsArgumentPresent(argument_mapping, VwapFunction::TargetPOV)),
    maybe_by_ticks(IsArgumentPresent(argument_mapping, VwapFunction::Ticks)),
    maybe_by_markouts(IsArgumentPresent(argument_mapping, VwapFunction::Markouts)),
    markouts_size(0),
    type(maybe_by_time && false == maybe_by_pov && false == maybe_by_ticks && false == maybe_by_markouts ? Type::ByTime
      : false == maybe_by_time && maybe_by_pov && false == maybe_by_ticks && false == maybe_by_markouts ? Type::ByPov
      : false == maybe_by_time && false == maybe_by_pov && maybe_by_ticks && false == maybe_by_markouts ? Type::ByTicks
      : false == maybe_by_time && false == maybe_by_pov && false == maybe_by_ticks && maybe_by_markouts ? Type::ByMarkouts
      : Type::Unknown) {
        if (type == Type::Unknown) {
          throw Exception(ErrorType::InvalidArgument);
        }
      }

void ExecutionPlan::Input(tick_calc::InputRecord & input_record) {
  try {
    const auto & symbol = input_record.values[argument_mapping[VwapFunction::Symbol]];
    const Date date = MkTaqDate(input_record.values[argument_mapping[VwapFunction::Date]]);
    InputRecordRange& input_range = input_record_ranges[make_pair(string(symbol.data(), symbol.size()), date)];
    const Time start_time = MkTime(input_record.values[argument_mapping[VwapFunction::StartTime]]);
    const char side = IsArgumentPresent(argument_mapping, VwapFunction::Side)
      ? DecodeSide(input_record.values[argument_mapping[VwapFunction::Side]]) : '\0';
    const Double limit_price = side && IsArgumentPresent(argument_mapping, VwapFunction::LimitPx)
      ? Double(input_record.values[argument_mapping[VwapFunction::LimitPx]]) : Double();
    const Flavor flavor = IsArgumentPresent(argument_mapping, VwapFunction::Flavor)
      ? DecodeFlavor(input_record.values[argument_mapping[VwapFunction::Flavor]]) : Flavor::Regular;

    if (flavor == Flavor::Unknown) {
      throw Exception(ErrorType::InvalidArgument);
    }

    if (type == Type::ByTime) {
      const Time end_time = MkTime(input_record.values[argument_mapping[VwapFunction::EndTime]]);
      input_range.emplace_back(input_record.id, start_time, end_time - start_time, side, limit_price, flavor);
    }
    else if (type == Type::ByMarkouts) {
      vector<Duration> durations = DecodeMarkouts(input_record.values[argument_mapping[VwapFunction::Markouts]]);
      if (durations.empty()) {
        throw Exception(ErrorType::InvalidArgument);
      }
      else if (markouts_size == 0) {
        markouts_size = durations.size();
      }
      else if (durations.size() != markouts_size) {
        throw Exception(ErrorType::InvalidArgument); // number of markouts entries must be consistent throughout all input records
      }
      input_range.emplace_back(input_record.id, start_time, durations, side, limit_price, flavor);
    }
    else if (type == Type::ByPov) {
      const uint64_t vol = TextToNumeric<uint64_t>(input_record.values[argument_mapping[VwapFunction::TargetVolume]]);
      const double pov = TextToNumeric<double>(input_record.values[argument_mapping[VwapFunction::TargetPOV]]);
      input_range.emplace_back(input_record.id, start_time, DurationEnding::Pov(vol, pov), side, limit_price, flavor);
    }
    else if (type == Type::ByTicks) {
      const int ticks = TextToNumeric<int>(input_record.values[argument_mapping[VwapFunction::Ticks]]);
      input_range.emplace_back(input_record.id, start_time, ticks, side, limit_price, flavor);
    }
  }
  catch (const Exception& Ex) {
    Error(Ex.errtype());
  }
  catch (...) {
    Error(ErrorType::InvalidArgument);
  }
}

void ExecutionPlan::SetResultFieldsForMarkouts() {
  if (markouts_size) {
    result_fields.emplace_back("ID");
    for (size_t i = 1; i <= markouts_size; ++ i) {
      for (const string &field : function.output_fields) {
        if (field != "ID") {
          char buf[32];
          #ifdef __unix__
          sprintf(buf, "%s_%lu", field.c_str(), i);
          #else
          sprintf_s(buf, sizeof(buf), "%s_%llu", field.c_str(), i);
          #endif
          result_fields.emplace_back(buf);
        }
      }
    }
  }
  else {
    result_fields = function.output_fields;
  }
}

void ExecutionPlan::Execute() {
  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto& range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  sort(slices.begin(), slices.end(), [](const InputRecordSlice& left, const InputRecordSlice& right) {
    return (get<2>(left)->size() > get<2>(right)->size());
    });
  for (auto& slice : slices) {
    shared_ptr<tick_calc::ExecutionUnit> job = make_shared<ExecutionUnit>(
      get<0>(slice), get<1>(slice), request.tz_name == "UTC", request.input_sorted , *get<2>(slice)
      );
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
  SetResultFieldsForMarkouts();
}

}
}
