#include <boost/algorithm/string.hpp>
#include "taq-proc.h"
#include "tick-func.h"

#include "tuple"
#include "algorithm"

using namespace std;
using namespace Taq;

namespace tick_calc {

void QuoteExecutionPlan::QuoteExecutionUnit::Execute() {
  ostringstream ss;
  ss << "thread-id:" << this_thread::get_id() << " type_name:"  << typeid(*this).name()
       << " symbol: " << symbol << " date:" << boost::gregorian::to_iso_extended_string(date)
       << " size:" << input_records.size();
  //cout << " from worker  " << ss.str() << endl;
  output_records.push_back(OutputRecord(1, ss.str()));
  //for (auto rec : input_records) {
  //}
}

void QuoteExecutionPlan::Input(InputRecord& input_record) {
  //vector<string> {"Symbol", "Timestamp"},
  const string & symbol = input_record.values[argument_mapping[0]];
  const string & timestamp = input_record.values[argument_mapping[1]];
  vector<string> values;
  boost::split(values, timestamp, boost::is_any_of("T"));
  if (values.size() ==2) {
    const Date date = MkDate(values[0]);
    const Time time = MkTime(values[1]);
    InputRecordRange & input_range = input_record_ranges[make_pair(symbol, date)];
    input_range.push_back(QuoteExecutionUnit::InputRecord(input_record.id, time));
  }
};

void QuoteExecutionPlan::Execute() {
  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto & range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  try {
  sort(slices.begin(), slices.end(),[] (const InputRecordSlice &left, const InputRecordSlice &right) {
    return (get<2>(left)->size() < get<2>(right)->size());
  });
  } catch (exception & ex) {
    cout << ex.what() << endl;
  }
  for (auto& slice : slices) {
    shared_ptr<ExecutionUnit> job = make_shared<QuoteExecutionUnit>(get<0>(slice), get<1>(slice), move(*get<2>(slice)));
    //job->Execute();
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

}
