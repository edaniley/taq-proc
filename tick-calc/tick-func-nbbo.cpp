#include "tuple"
#include "algorithm"
#include "iterator"

#include "taq-proc.h"
#include "taq-text.h"
#include "taq-double.h"
#include "tick-calc.h"
#include "tick-func-nbbo.h"

using namespace std;
using namespace Taq;

namespace tick_calc {
namespace NBBO {

class NBBO : public  FunctionDefinition {
public:
  enum ArgumentNo { Symbol, Timestamp, Markouts };
  NBBO(const string& name, const vector<string>& argument_names, const vector<string>& output_fields) :
    FunctionDefinition(name, argument_names, output_fields) {}
  void ValidateArgumentList(const vector<int>& argument_mapping) const override {
    bool valid = IsArgumentPresent(argument_mapping, Symbol) && IsArgumentPresent(argument_mapping, Timestamp);
    if (!valid) {
      throw invalid_argument("Invalid argument list");
    }
  }
};

static const bool registered[] = {
  RegisterFunctionDefinition(make_unique<NBBO>("NBBO",
    vector<string> {"Symbol", "Timestamp", "Markouts"},
    vector<string> {"ID", "Timestamp", "BestBidPx", "BestBidQty", "BestOfferPx", "BestOfferQty"})
    ),
  RegisterFunctionDefinition(make_unique<NBBO>("NBBOPrice",
    vector<string> {"Symbol", "Timestamp", "Markouts"},
    vector<string> {"ID", "Timestamp", "BestBidPx", "BestOfferPx"})
  )
};

template<typename T>
const T* FindNbboByTime(const SortedConstVector<T>& quotes, const T* it, Time reference_time, Time time_offset) {
  const T* retval = nullptr;
  const Time target_time = reference_time + time_offset;
  if (reference_time <= target_time) { // forward
    for (; it < quotes.end() && it->time <= target_time; ++ it) {
      retval = it;
    }
  }
  else if (it < quotes.end()) { // reverse
    while (it->time > target_time && it > quotes.begin()) {
      -- it;
    }
    if (it->time <= target_time) {
      retval = it;
    }
  }
  return retval;
}

template<typename T>
const T* FindNbboByTick(const SortedConstVector<T>& quotes, const T* it, int ticks_offset) {
  const T* retval = it < quotes.end() ? it : nullptr;
  int target_cnt = abs(ticks_offset);
  if (ticks_offset >= 0) { // forward
    for (; target_cnt > 0 && it != quotes.end(); ++ it, target_cnt --) {
      if (it != quotes.end())
        retval = it;
    }
  }
  else if (it != quotes.end()) { // reverse
    for (; target_cnt > 0 && it > quotes.begin(); -- it, target_cnt --) {
      retval = it;
    }
  }
  return retval;
}

template<typename T>
const T* FindNbbo(const SortedConstVector<T>& quotes, const T* it, Time reference_time, const Duration &offset) {
  const T* retval = nullptr;
  if (offset.type == DurationType::Time) {
    retval = FindNbboByTime<T>(quotes, it, reference_time, offset.ending.time);
  } else if (offset.type == DurationType::Ticks) {
    retval = FindNbboByTick<T>(quotes, it, offset.ending.ticks);
  }
  return retval;
}

void PrintNbbo(ostringstream &ss, const Nbbo &nbbo, int lot_size) {
  ss << '|' << nbbo.time << '|' << nbbo.bidp << '|' << (nbbo.bids * lot_size) << '|' << nbbo.askp << '|' << (nbbo.asks * lot_size);
}
void PrintNbbo(ostringstream& ss) {
  ss << '|' << Taq::ZeroTime() << '|' << Double() << "|0|" << Double() << "|0";
}

void PrintNbboPrice(ostringstream& ss, const NbboPrice& nbbo) {
  ss << '|' << nbbo.time << '|' << nbbo.bidp << '|' << nbbo.askp;
}

void PrintNbboPrice(ostringstream& ss) {
  ss << '|' << Taq::ZeroTime() << '|' << Double() << '|' << Double();
}

void PriceQuantityExecutionUnit::Execute() {
  auto & secmaster_mgr = SecurityMasterManager();
  auto & quote_mgr = NbboRecordsetManager();
  const SecMaster* secmaster = nullptr;
  const SymbolRecordset<Nbbo>* symbol_recordset = nullptr;
  int lot_size = 100;
  try {
    secmaster = &secmaster_mgr.Load(date);
    const Security &security = secmaster->FindBySymbol(symbol);
    lot_size = security.lot_size;
    symbol_recordset = &quote_mgr.LoadSymbolRecordset(date, security.symb);
  }
  catch (...) {
    Error(ErrorType::DataNotFound, (int)input_records.size());
    return;
  }
  if (false == input_sorted) {
    sort(input_records.begin(), input_records.end(), [] (const auto &lh, const auto& rh) {return lh.time < rh.time;});
  }
  auto & quotes = symbol_recordset->records;
  auto it = quotes.begin();
  for (auto rec : input_records) {
    const Time requested_time = rec.time + taq_time_adjustment;
    it = quotes.find_prior(it, quotes.end(), requested_time);
    if (it != quotes.end()) {
      ostringstream ss;
      ss << rec.id;
      PrintNbbo(ss, *it, lot_size);
      for (const auto & duration: rec.durations) {
        if (const Nbbo* nbbo = FindNbbo<Nbbo>(quotes, it, requested_time, duration)) {
          PrintNbbo(ss, *nbbo, lot_size);
        } else {
          PrintNbbo(ss);
        }
      }
      ss << endl;
      output_records.emplace_back(rec.id, ss.str());
    } else {
      Error(ErrorType::DataNotFound);
    }
  }
  quote_mgr.UnloadSymbolRecordset(date, symbol);
  secmaster_mgr.Release(*secmaster);
}


void PriceOnlyExecutionUnit::Execute() {
  auto& quote_mgr = NbboPoRecordsetManager();
  const SymbolRecordset<NbboPrice>* symbol_recordset = nullptr;
  try {
    symbol_recordset = &quote_mgr.LoadSymbolRecordset(date, symbol);
  }
  catch (...) {
    Error(ErrorType::DataNotFound, (int)input_records.size());
    return;
  }
  if (false == input_sorted) {
    sort(input_records.begin(), input_records.end(), [](const auto& lh, const auto& rh) {return lh.time < rh.time; });
  }
  auto& quotes = symbol_recordset->records;
  auto it = quotes.begin();
  for (auto rec : input_records) {
    const Time requested_time = rec.time + taq_time_adjustment;
    it = quotes.find_prior(it, quotes.end(), requested_time);
    if (it != quotes.end()) {
      ostringstream ss;
      ss << rec.id;
      PrintNbboPrice(ss, *it);
      for (const auto& duration : rec.durations) {
        if (const NbboPrice* nbbo = FindNbbo<NbboPrice>(quotes, it, requested_time, duration)) {
          PrintNbboPrice(ss, *nbbo);
        }
        else {
          PrintNbbo(ss);
        }
      }
      ss << endl;
      output_records.emplace_back(rec.id, ss.str());
    }
    else {
      Error(ErrorType::DataNotFound);
    }
  }
  quote_mgr.UnloadSymbolRecordset(date, symbol);
}

ExecutionPlan::ExecutionPlan(const FunctionDefinition& function, const Request& request, const vector<int>& argument_mapping, Type type)
  : tick_calc::ExecutionPlan(function, request, argument_mapping),
  type(type), with_markouts(IsArgumentPresent(argument_mapping, NBBO::Markouts)), markouts_size(0) {}


void ExecutionPlan::Input(tick_calc::InputRecord& input_record) {
  const auto & symbol = input_record.values[argument_mapping[NBBO::Symbol]];
  const auto & timestamp = input_record.values[argument_mapping[NBBO::Timestamp]];
  vector<string_view> values;
  Split(values, timestamp, 'T');
  if (values.size() ==2) {
    const Date date = MkDate(values[0]);
    const Time time = MkTime(values[1]);
    InputRecordRange& input_range = input_record_ranges[make_pair(string(symbol.data(), symbol.size()), date)];
    if (with_markouts) {
      vector<Duration> durations = DecodeMarkouts(input_record.values[argument_mapping[NBBO::Markouts]]);
      if (durations.empty()) {
        throw Exception(ErrorType::InvalidArgument); // markouts must have one or more entry
      }
      if (markouts_size == 0)
        markouts_size = durations.size();
      else if (durations.size() != markouts_size) {
        throw Exception(ErrorType::InvalidArgument); // number of markouts entries must be consistent throughout all input records
      }
      input_range.emplace_back(input_record.id, time, durations);
    } else {
      input_range.emplace_back(input_record.id, time);
    }
  }
}

void ExecutionPlan::Execute() {
  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto & range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  sort(slices.begin(), slices.end(),[] (const InputRecordSlice &left, const InputRecordSlice &right) {
    return (get<2>(left)->size() > get<2>(right)->size());
  });
  for (auto& slice : slices) {
    shared_ptr<tick_calc::ExecutionUnit> job;
    if (type == Type::PriceAndQty) {
      job = make_shared<PriceQuantityExecutionUnit>(get<0>(slice), get<1>(slice), request.tz_name == "UTC", request.input_sorted, move(*get<2>(slice)));
    } else {
      job = make_shared<PriceOnlyExecutionUnit>(get<0>(slice), get<1>(slice), request.tz_name == "UTC", request.input_sorted, move(*get<2>(slice)));
    }
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
  SetResultFieldsForMarkouts();
}

void ExecutionPlan::SetResultFieldsForMarkouts() {
  result_fields = function.output_fields;
  if (markouts_size) {
    for (size_t i = 1; i <= markouts_size; ++i) {
      for (const string& field : function.output_fields) {
        if (field != "ID") {
          char buf[64];
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

}
}
