#include "tuple"
#include "algorithm"
#include "iterator"

#include "boost-algorithm-string.h"
#include "taq-proc.h"
#include "tick-func.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

void QuoteExecutionPlan::QuoteExecutionUnit::Execute() {
  auto & secmaster_mgr = SecurityMasterManager();
  auto & quote_mgr = QuoteRecordsetManager();
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
  const Time taq_time_adjustment = adjust_time ? UtcToTaq(date) : ZeroTime();
  auto & quotes = symbol_recordset->records;
  auto it = quotes.begin();
  for (auto rec : input_records) {
    const Time requested_time = rec.time + taq_time_adjustment;
    it = quotes.lower_bound(it, quotes.end(), requested_time);
    if (it != quotes.end()) {
      ostringstream ss;
      if (requested_time < it->time && it != quotes.begin()) {
        --it;
      }
      ss << rec.id << '|' << it->time << '|' << it->bidp << '|' << (it->bids * lot_size)
                                      << '|' << it->askp << '|' << (it->asks * lot_size) << endl;
      output_records.emplace_back(rec.id, ss.str());
    } else {
      Error(ErrorType::DataNotFound);
    }
  }
  quote_mgr.UnloadSymbolRecordset(date, symbol);
  secmaster_mgr.Release(*secmaster);
}

void QuoteExecutionPlan::Input(InputRecord& input_record) {
  const string & symbol = input_record.values[argument_mapping[0]];
  const string & timestamp = input_record.values[argument_mapping[1]];
  vector<string> values;
  boost::split(values, timestamp, boost::is_any_of("T "));
  if (values.size() ==2) {
    const Date date = MkDate(values[0]);
    const Time time = MkTime(values[1]);
    InputRecordRange& input_range = input_record_ranges[make_pair(symbol, date)];
    input_range.emplace_back(input_record.id, time);
  }
}

void QuoteExecutionPlan::Execute() {
  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto & range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  sort(slices.begin(), slices.end(),[] (const InputRecordSlice &left, const InputRecordSlice &right) {
    return (get<2>(left)->size() > get<2>(right)->size());
  });
  for (auto& slice : slices) {
    shared_ptr<ExecutionUnit> job = make_shared<QuoteExecutionUnit>(
      get<0>(slice), get<1>(slice), request.input_sorted, request.tz_name == "UTC", move(*get<2>(slice))
    );
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

}
