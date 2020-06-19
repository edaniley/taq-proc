#include <tuple>
#include <algorithm>
#include <iterator>
#include <cmath>

#include <boost/algorithm/string.hpp>
#include "taq-proc.h"
#include "tick-func.h"
#include "double.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

using RestType = RodExecutionPlan::RestType;
static mutex cout_mtx;
struct RodSlice{
  RodSlice(Time start_time, Time end_time, int leaves_qty)
    : start_time(start_time), end_time(end_time), leaves_qty(leaves_qty) { }
  const Time start_time;
  const Time end_time;
  const int leaves_qty;
};

static RestType DecodeRestType(const string& str) {
  if (false == str.empty()) {
    const int val = stoi(str);
    if (val >= -(int)RestType::Zero && val <= (int)RestType::Zero) {
      return (RestType)(val + (int)RestType::Zero);
    }
  }
  return RestType::None;
}

static RestType InvertRestType(const RestType mpa) {
  return (RestType)(-1 * ((int)mpa - (int)RestType::Zero) + (int)RestType::Zero);
}

static RestType RestingType(const NbboPrice &nbbo, char side, const Double &limit_price, const RestType &mpa) {
  if (limit_price.Empty() || limit_price.IsZero() || mpa == RestType::MinusThree) {
    return mpa;
  }
  RestType retval = RestType::None;
  const bool valid_nbbo = nbbo.bidp > 0 && nbbo.askp < numeric_limits<double>::max();
  if (valid_nbbo) {
    const Double bid(nbbo.bidp);
    const Double offer(nbbo.askp);
    const Double mid(.5 * (nbbo.bidp + nbbo.askp));
    if (limit_price.Less(bid)) {
      retval = RestType::MinusThree;
    } else if (limit_price.Equal(bid)) {
      retval = RestType::MinusTwo;
    } else if (limit_price.Greater(bid) && limit_price.Less(mid)) {
      retval = RestType::MinusOne;
    } else if (limit_price.Equal(mid)) {
      retval = RestType::Zero;
    } else if (limit_price.Greater(mid) && limit_price.Less(offer)) {
      retval = RestType::PlusOne;
    } else if (limit_price.Equal(offer)) {
      retval = RestType::PlusTwo;
    } else {
      retval = RestType::PlusThree;
    }
    if (mpa == RestType::PlusThree) {
      retval = min(retval, RestType::Zero);
    } else if (mpa != RestType::None) {
      retval = min(retval, mpa);
    }
    retval = side == 'B' ? retval : InvertRestType(retval);
  }
  return retval;
}

static void CalculateROD(vector<double> &result, const NbboPrice*quote_start, const NbboPrice* quote_end,
                        const vector<RodSlice> slices, char side, const Double &limit_price, const RestType &mpa) {
    auto slice = slices.begin();
    const NbboPrice* current_quote = quote_start;
    //cout << "CalculateROD slices:" << slices.size() << " quote_start:" << quote_start->time << " quote_end:" << quote_end->time  << endl;
    while (slice != slices.end()) {
      const Time start_time = max(slice->start_time, current_quote->time); // latest of slice start time or curent nbbo time
      const NbboPrice* next_quote = (current_quote + 1) < quote_end ? current_quote + 1 : nullptr;
      const Time end_time = next_quote            // check if subsequent quote is present
        ? min(slice->end_time, next_quote->time)  // earlierst of end of slice or nbbo change i.e. next nbbo time
        : slice->end_time;
      //ostringstream ss; ss << setprecision(4);
      //ss << "slice start:" << start_time << " end:" << end_time
      //  << "\ncurr [ " << current_quote->time << " " << current_quote->bidp << " : " << current_quote->askp << " ]";
      //if (next_quote)
      //  ss  << "\nnext [ " << next_quote->time << " " << next_quote->bidp << " : " << next_quote->askp << " ]";
      //cout << ss.str() << endl;

      const RestType rest_type = RestingType(*current_quote, side, limit_price, mpa);
      if (rest_type != RestType::None) {
        auto& shares_per_second = result[(int)rest_type];
        shares_per_second += .000001 * (end_time - start_time).total_microseconds() * slice->leaves_qty;
      }
      if (next_quote && next_quote->time < slice->end_time) {
        current_quote = next_quote;
      } else {
        ++ slice;
      }
    }
}

void RodExecutionPlan::RodExecutionUnit::Execute() {
  auto started = pt::microsec_clock::local_time();
  {
    unique_lock<mutex> lock(cout_mtx);
    cout << started << " thread-id:" << this_thread::get_id() << " started symbol:" << symbol
         << " record-cnt:" << input_records.size() << endl;
  }
  auto& quote_mgr = NbboPoRecordsetManager();
  const SymbolRecordset<NbboPrice> * symbol_recordset = nullptr;
  try {
    symbol_recordset = &quote_mgr.LoadSymbolRecordset(date, symbol);
  } catch (const exception & ex) {
    unique_lock<mutex> lock(cout_mtx);
    cout << started << " thread-id:" << this_thread::get_id() << " error:" << ex.what() << endl;
    return;
  }
  auto & quotes = symbol_recordset->records;
  auto quote_start = quotes.begin();
  for (auto rec : input_records) {
    ostringstream ss;
    try {
      quote_start = quotes.find_prior(quote_start, quotes.end(), rec.start_time);
      if (quote_start == quotes.end()) {
        throw domain_error("Marke data not found");
      }
      vector<RodSlice> slices;
      if (rec.executions.empty()) {
        // no executions
        slices.emplace_back(rec.start_time, rec.end_time, rec.ord_qty);
      } else {
        // split order duration into execution count + 1 slices (start and end times, and leaves qty)
        int leaves_qty = rec.ord_qty;
        Time start_time = rec.start_time;
        Time end_time;
        for (auto exec : rec.executions) {
          const Time exec_time = exec.first;
          const int exec_qty = exec.second;
          end_time = exec_time;
          if (end_time < start_time) {
            throw invalid_argument("Out of sequence timestamp");
          }
          else if (end_time > start_time) {
            slices.emplace_back(start_time, end_time, leaves_qty);
            start_time = end_time;
          }
          leaves_qty -= exec_qty;
        }
        if (leaves_qty < 0) {
          throw invalid_argument("Sum of execution quantity exceeds order quantity");
        } else if (leaves_qty > 0 and start_time < rec.end_time) {
          // create final slice with time remaining and non-zero LeavesQty
          slices.emplace_back(start_time, rec.end_time, leaves_qty);
        }
      }
      //cout << "-order start_time:" << rec.start_time << " end_time:" << rec.end_time << " res_qty:" << rec.ord_qty << endl;
      //for (auto & r : slices) {
      //  cout << "+range start_time:" << r.start_time << " end_time:" << r.end_time << " res_qty:" << r.leaves_qty << endl;
      //}
      //cout << endl;

      // calculate rod here 
      auto quote_end = quotes.upper_bound(quote_start, quotes.end(), slices.rbegin()->end_time);
      vector<double> rod_values((size_t)RestType::Max, .0);
      CalculateROD(rod_values, quote_start, quote_end, slices, rec.side, rec.limit_price, rec.mpa);
      ss << "rec-id:" << rec.id << " usr-id:" << rec.order_id;
      for (size_t i = 0; i < rod_values.size(); i ++) {
        ss << '|' << rod_values[i];
      }
      ss << endl;
    } catch (exception & ex) {
      ss << ex.what() << endl;
    }
    //if (it != quotes.end()) {
    //  if (rec.time < it->time && it != quotes.begin()) {
    //    --it;
    //  }
    //  ss << "id:" << rec.id << " symbol:" << symbol << " time:" << rec.time << " quote-time:" << it->time
    //    << " bidp:" << it->bidp << " bids:" << it->bids
    //    << " askp:" << it->askp << " asks:" << it->asks << endl;
    //}
    //else {
    //  ss << "id:" << rec.id << " time" << rec.time << " quote not found" << endl;
    //}
    //cout << "id:" << rec.id << " values:" << ss.str();
    output_records.emplace_back(rec.id, ss.str());
  }

  quote_mgr.UnloadSymbolRecordset(date, symbol);
  auto finished = pt::microsec_clock::local_time();
  {
    unique_lock<mutex> lock(cout_mtx);
    cout << started << " thread-id:" << this_thread::get_id() << " finished symbol:" << symbol
      << " run-time:" << (finished - started) << endl;
  }
}

void RodExecutionPlan::Input(InputRecord& input_record) {
  //"ID", "Symbol", "Date", "StartTime", "EndTime", "Side", "OrdQty", "LimitPrice", "MPA"
  bool invalid = input_record.values[1].empty()  // symbol
              || input_record.values[2].empty()  // date
              || input_record.values[3].empty()  // start time
              || input_record.values[4].empty()  // end time
              || input_record.values[5].empty()  // side
              || input_record.values[6].empty(); // order qty
  if (invalid) {
    error_cnt ++;
    return;
  }
  const string& id= input_record.values[0];
  const string& symbol = input_record.values[1];
  const Date date = MkDate(input_record.values[2]);
  const Time start_time = MkTime(input_record.values[3]);
  const Time end_time = MkTime(input_record.values[4]);
  const char side = (char)toupper(input_record.values[5][0]);
  const int ord_qty = stoi(input_record.values[6]);
  const Double limit_price(input_record.values[7]);
  const RestType mpa = DecodeRestType(input_record.values[8]);
  InputRecordRange& input_range = input_record_ranges[make_pair(symbol, date)];
  input_range.emplace_back(input_record.id, id, start_time, end_time, side, ord_qty, limit_price, mpa);
  //cout << "symbol:" << symbol << " date:" << date << " start_time:" << start_time << " end_time:" << end_time
  //  << " side:" << side << " ord_qty:" << ord_qty << " limit_price:" << limit_price
  //  << " mpa:" << ToString(mpa) << endl;
  static const size_t base_size = 9;
  if (input_record.values.size() > base_size) {
    if (input_record.values.size() % 2 == 0) {
      throw invalid_argument("Incomplete executions data");
    }
    auto & executions = input_range.rbegin()->executions;
    //ostringstream ss;
    for (size_t j = base_size; j < input_record.values.size(); j += 2) {
      if (input_record.values[j].empty() || input_record.values[j + 1].empty()) {
        error_cnt ++;
        return;
      }
      const Time exec_time = MkTime(input_record.values[j]);
      const int exec_qty = stoi(input_record.values[j+1]);
      executions.emplace_back(exec_time, exec_qty);
      //ss << " exec_time:" << exec_time << " exec_qty:" << exec_qty;
    }
    //cout << ss.str() << endl;
  }
}

void RodExecutionPlan::Execute() {
  auto started = pt::microsec_clock::local_time();
  {
    unique_lock<mutex> lock(cout_mtx);
    cout << started << " thread-id:" << this_thread::get_id() << " Starting execution; input error_cnt:" << error_cnt << endl;
  }

  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto& range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  sort(slices.begin(), slices.end(), [](const InputRecordSlice& left, const InputRecordSlice& right) {
    return (get<2>(left)->size() < get<2>(right)->size());
    });
  for (auto& slice : slices) {
    shared_ptr<ExecutionUnit> job = make_shared<RodExecutionUnit>(get<0>(slice), get<1>(slice), move(*get<2>(slice)));
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

ExecutionPlan::State RodExecutionPlan::CheckState() {
  // move every completed unit from todo_list to done_list
  for (auto it = todo_list.begin(); it != todo_list.end(); ) {
    if ((*it)->done.load()) {
      done_list.push_back(*it);
      it = todo_list.erase(it);
    }
    else {
      ++it;
    }
  }
  if (todo_list.empty() && output_records.empty()) {
    // once : consolidate all output records into output_records, then sort by id
    size_t total_record_count = 0;
    for_each(done_list.begin(), done_list.end(), [&total_record_count](const auto& exec_unit) {
      total_record_count += exec_unit->output_records.size();
      });
    output_records.reserve(total_record_count);
    for (auto& exec_unit : done_list) {
      output_records.insert(output_records.end(),
        make_move_iterator(exec_unit->output_records.begin()),
        make_move_iterator(exec_unit->output_records.end()));
      exec_unit->output_records.clear();
    }
    sort(output_records.begin(), output_records.end(), [](const auto& left, const auto& right) {
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

int RodExecutionPlan::PullOutput(char* buffer, int available_size) {
  int bytes_written = 0;
  if (buffer && available_size && output_records_done < output_records.size()) {
    const OutputRecord& rec = output_records[output_records_done];
    if (available_size >= (int)rec.value.size()) {
      memcpy(buffer, rec.value.c_str(), rec.value.size());
      output_records_done++;
      bytes_written = (int)rec.value.size();
    }
  }
  return bytes_written;
}

}
