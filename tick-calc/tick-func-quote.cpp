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
  // auto started = pt::microsec_clock::local_time();
  // cout << started << " thread-id:" << this_thread::get_id() << " started symbol:" << symbol
  //   << " record-cnt:" << input_records.size() << endl;
  auto & quote_mgr = QuoteRecordsetManager();
  const tick_calc::SymbolRecordset<Nbbo>& symbol_recordset = quote_mgr.LoadSymbolRecordset(date, symbol);
  auto & quotes = symbol_recordset.records;
  auto it = quotes.begin();
  for (auto rec : input_records) {
    it = quotes.lower_bound(it, quotes.end(), rec.time);
    if (it != quotes.end()) {
      ostringstream ss;
      if (rec.time < it->time && it != quotes.begin()) {
        --it;
      }
      ss << rec.id << '|' << it->time << '|' << it->bidp << '|' << it->bids << '|' << it->askp << '|' << it->asks << endl;
      //ss << "id|" << rec.id << "|symbol|" << symbol << "|time|" << rec.time << "|quote-time|" << it->time
      //  << "|bidp|" << it->bidp << "|bids|" << it->bids
      //  << "|askp|" << it->askp << "|asks|" << it->asks << endl;

      output_records.emplace_back(rec.id, ss.str());
    } else {
      Error(ErrorType::DataNotFound);
    }
  }
  quote_mgr.UnloadSymbolRecordset(date, symbol);
  // auto finished = pt::microsec_clock::local_time();
  // cout << started << " thread-id:" << this_thread::get_id() << " finished symbol:" << symbol
  //   << " run-time:" << (finished- started) << endl;
}

void QuoteExecutionPlan::Input(InputRecord& input_record) {
  const string & symbol = input_record.values[argument_mapping[0]];
  const string & timestamp = input_record.values[argument_mapping[1]];
  vector<string> values;
  boost::split(values, timestamp, boost::is_any_of("T"));
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
    return (get<2>(left)->size() < get<2>(right)->size());
  });
  for (auto& slice : slices) {
    shared_ptr<ExecutionUnit> job = make_shared<QuoteExecutionUnit>(get<0>(slice), get<1>(slice), move(*get<2>(slice)));
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

}
