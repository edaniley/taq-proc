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
  auto started = pt::microsec_clock::local_time();
  cout << started << " thread-id:" << this_thread::get_id() << " started symbol:" << symbol
    << " record-cnt:" << input_records.size() << endl;
  auto & quote_mgr = QuoteRecordsetManager();
  const tick_calc::SymbolRecordset<Nbbo>& symbol_recordset = quote_mgr.LoadSymbolRecordset(date, symbol);
  auto & quotes = symbol_recordset.records;
  auto it = quotes.begin();
  for (auto rec : input_records) {
    it = quotes.lower_bound(it, quotes.end(), rec.time);
    ostringstream ss;
    if (it != quotes.end()) {
      if (rec.time < it->time && it != quotes.begin()) {
        --it;
      }
      ss << "id|" << rec.id << "|symbol|" << symbol << "|time|" << rec.time << "|quote-time|" << it->time
        << "|bidp|" << it->bidp << "|bids|" << it->bids
        << "|askp|" << it->askp << "|asks|" << it->asks << endl;
    } else {
      ss << "id:" << rec.id << " time:" << rec.time << " quote not found" << endl;
    }
    output_records.emplace_back(rec.id, ss.str());
  }
  quote_mgr.UnloadSymbolRecordset(date, symbol);
  auto finished = pt::microsec_clock::local_time();
  cout << started << " thread-id:" << this_thread::get_id() << " finished symbol:" << symbol
    << " run-time:" << (finished- started) << endl;
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

ExecutionPlan::State QuoteExecutionPlan::CheckState() {
  // move every completed unit from todo_list to done_list
  for (auto it = todo_list.begin(); it != todo_list.end(); ) {
    if ((*it)->done.load()) {
      done_list.push_back(*it);
      it = todo_list.erase(it);
    } else {
      ++ it;
    }
  }
  if (todo_list.empty() && output_records.empty()) {
    // once : consolidate all output records into output_records, then sort by id
    size_t total_record_count = 0;
    for_each(done_list.begin(), done_list.end(), [&total_record_count](const auto & exec_unit) {
      total_record_count += exec_unit->output_records.size();
    });
    output_records.reserve(total_record_count);
    for (auto & exec_unit : done_list) {
      output_records.insert(output_records.end(),
        make_move_iterator(exec_unit->output_records.begin()),
        make_move_iterator(exec_unit->output_records.end()));
      exec_unit->output_records.clear();
    }
    sort(output_records.begin(), output_records.end(), [] (const auto & left, const auto& right) {
      return left.id < right.id;
    });
  }
  const auto unit_cnt = todo_list.size() + done_list.size();
  const bool busy = todo_list.size() > 0 || unit_cnt == 0;
  const bool ready = !busy && output_records.size() > 0 && output_records_done < output_records.size();
  ExecutionPlan::State state = busy ? ExecutionPlan::State::Busy
                             : ready ? ExecutionPlan::State::OuputReady : ExecutionPlan::State::Done;
  return state;
}

int QuoteExecutionPlan::PullOutput(char* buffer, int available_size) {
  int bytes_written = 0;
  if (buffer) {
    while (output_records_done < output_records.size()) {
      const OutputRecord & rec = output_records[output_records_done];
      const int rec_size = (int)rec.value.size();
      if (available_size < rec_size) {
        break;
      }
      memcpy(buffer, rec.value.c_str(), rec.value.size());
      output_records_done ++;
      buffer += rec_size;
      bytes_written += rec_size;
      available_size -= rec_size;
    }
  }
  return bytes_written;
}

}
