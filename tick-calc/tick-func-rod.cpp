#include "tuple"
#include "algorithm"
#include "iterator"

#include <boost/algorithm/string.hpp>
#include "taq-proc.h"
#include "tick-func.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

using RestType = RodExecutionPlan::RestType;
using PegType = RodExecutionPlan::PegType;

struct RodSlice{
  RodSlice(Time start_time, Time end_time, int leaves_qty)
    : start_time(start_time), end_time(end_time), leaves_qty(leaves_qty) { }
  const Time start_time;
  const Time end_time;
  const int leaves_qty;
};

static RestType RestingType(const Nbbo &nbbo, char side, double limit_price, PegType peg_type, RestType mpa) {
  const bool valid_nbbo = nbbo.bidp > 0 && nbbo.askp < numeric_limits<double>::max();
  const double mid = valid_nbbo ? (nbbo.bidp + nbbo.askp) / 2 : 0;
  double peg_price = 0;
  if (valid_nbbo ) {
    if (peg_type == PegType::Midpoint) {
      peg_price = mid;
    } else if (peg_type == PegType::Primary) {
      peg_price = side == 'B' ? nbbo.bidp : nbbo.askp;
    } else if (peg_type == PegType::Market) {
      peg_price = side == 'B' ? nbbo.askp : nbbo.bidp;
    }
  }

  if (limit_price == 0 && peg_price == 0) {
    return mpa == RestType::PlusThree ? RestType::PlusTwo : RestType::None;
  } else if (limit_price == 0) {
    limit_price = peg_price;
  } else if (peg_price == 0) {
    peg_price = limit_price;
  }

  const double eff_price = side == 'B' ? min(peg_price, limit_price) : max(peg_price, limit_price);
  int val = numeric_limits<int>::min();
  if (eff_price < nbbo.bidp) {
    val = -3;
  } else if (eff_price == nbbo.bidp) {
    val = -2;
  } else if (eff_price > nbbo.bidp && eff_price < mid) {
    val = -1;
  } else if (eff_price == mid) {
    val = 0;
  } else if (eff_price > mid && eff_price < nbbo.askp) {
    val = 1;
  } else if (eff_price == nbbo.askp) {
    val = 2;
  } else if (eff_price > nbbo.askp) {
    val = 3;
  }

  RestType retval = RestType::None;
  if (val != numeric_limits<int>::min()) {
    retval = (RestType)((side == 'B' ? val : -1*val) + 3);
  }
  return retval;
}


static void CalculateROD(vector<double> &result, const Nbbo *quote_start, const Nbbo* quote_end,
                        const vector<RodSlice> slices, char side, double limit_price, PegType peg_type, RestType mpa) {
    auto slice = slices.begin();
    const Nbbo* current_quote = quote_start;
    //Time start_time, end_time;
    while (slice != slices.end()) {
      const Time start_time = max(slice->start_time, current_quote->time); // latest of slice start time or curent nbbo time
      const Nbbo* next_quote = (current_quote + 1) < quote_end ? current_quote + 1 : nullptr;
      const Time end_time = next_quote            // check if subsequent quote is present
        ? min(slice->end_time, next_quote->time)  // earlierst of end of slice or nbbo change i.e. next nbbo time
        : slice->end_time;
      //ostringstream ss;
      //ss << "slice start:" << slice->start_time << " end:" << slice->end_time
      //<< " ; nbbo current::" << current_quote->time << " next:" << (next_quote ? next_quote->time : Time()) << endl;
      //cout << ss.str();
      const RestType rest_type = RestingType(*current_quote, side, limit_price, peg_type, mpa);
      if (rest_type != RestType::None) {
        auto& shares_per_second = result[(int)rest_type];
        shares_per_second += .0000001 * (end_time - start_time).total_microseconds() * slice->leaves_qty;
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
  cout << started << " thread-id:" << this_thread::get_id() << " started symbol:" << symbol
       << " record-cnt:" << input_records.size() << endl;
  auto& quote_mgr = QuoteRecordsetManager();
  const tick_calc::SymbolRecordset<Nbbo>& symbol_recordset = quote_mgr.LoadSymbolRecordset(date, symbol);
  auto & quotes = symbol_recordset.records;
  auto quote_start = quotes.begin();
  for (auto rec : input_records) {
    ostringstream ss;
    try {
      quote_start = quotes.find_prior(quote_start, quotes.end(), rec.start_time);
      if (quote_start == quotes.end()) {
        throw domain_error("Marke data not found");
      }
      auto quote_end = quotes.upper_bound(quote_start, quotes.end(), rec.end_time);
      vector<RodSlice> slices;
      if (rec.executions.size()) { // split order duration into execution count + 1 slices (start and end times, and leaves qty)
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
          slices.emplace_back(start_time, end_time, leaves_qty);
          start_time = end_time;
          leaves_qty -= exec_qty;
        }
        slices.emplace_back(start_time, rec.end_time, leaves_qty);
      } else {// no executions
        slices.emplace_back(rec.start_time, rec.end_time, rec.ord_qty);
      }
      //cout << "-order start_time:" << rec.start_time << " end_time:" << rec.end_time << " res_qty:" << rec.ord_qty << endl;
      //for (auto & r : slices) {
      //  cout << "+range start_time:" << r.start_time << " end_time:" << r.end_time << " res_qty:" << r.leaves_qty << endl;
      //}
      //cout << endl;

      // calculate rod here 
      vector<double> rod_values((size_t)RestType::Max, .0);
      CalculateROD(rod_values, quote_start, quote_end, slices, rec.side, rec.limit_price, rec.peg_type, rec.mpa);
      ss << "rec-id:" << rec.id;//rod_values[0];
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
  cout << started << " thread-id:" << this_thread::get_id() << " finished symbol:" << symbol
    << " run-time:" << (finished - started) << endl;
}

//static const char* ToString(PegType peg_type) {
//  switch (peg_type) {
//  case PegType::Primary: return "R";
//  case PegType::Midpoint: return "M";
//  case PegType::Market: return "P";
//  }
//  return "";
//}
//
//static const char* ToString(RestType mpa) {
//  switch (mpa) {
//  case RestType::MinusThree: return "-3";
//  case RestType::MinusTwo: return "-2";
//  case RestType::MinusOne: return "-1";
//  case RestType::Zero: return "0";
//  case RestType::PlusOne: return "1";
//  case RestType::PlusTwo: return "2";
//  case RestType::PlusThree: return "3";
//  }
//  return "";
//}

static PegType DecodePegType(const string& str) {
  if (false == str.empty()) {
    switch ((char)toupper(str[0])) {
    case 'R': return PegType::Primary;
    case 'M': return PegType::Midpoint;
    case 'P': return PegType::Market;
    }
  }
  return PegType::None;
}

static RestType DecodeMPA(const string &str) {
  if (false == str.empty()) {
    const int val = stoi(str);
    if (val >= -3 && val <= 3) {
      return (RestType)(val + 3);
    }
  }
  return RestType::None;
}

void RodExecutionPlan::Input(InputRecord& input_record) {
  const string& symbol = input_record.values[argument_mapping[0]];
  const Date date = MkDate(input_record.values[argument_mapping[1]]);
  const Time start_time = MkTime(input_record.values[argument_mapping[2]]);
  const Time end_time = MkTime(input_record.values[argument_mapping[3]]);
  const char side = (char)toupper(input_record.values[argument_mapping[4]][0]);
  const int ord_qty = stoi(input_record.values[argument_mapping[5]]);
  const string& LimitPrice = input_record.values[argument_mapping[6]];
  const double limit_price = LimitPrice.empty() ? 0 : stod(LimitPrice);
  PegType peg_type = DecodePegType(input_record.values[argument_mapping[7]]);
  const RestType mpa = DecodeMPA(input_record.values[argument_mapping[8]]);
  InputRecordRange& input_range = input_record_ranges[make_pair(symbol, date)];
  input_range.emplace_back(input_record.id, start_time, end_time, side, ord_qty, limit_price, peg_type, mpa);
  //cout << "symbol:" << symbol << " date:" << date << " start_time:" << start_time << " end_time:" << end_time
  //  << " side:" << side << " ord_qty:" << ord_qty << " limit_price:" << limit_price
  //  << " peg_type:" << ToString(peg_type) << " mpa:" << ToString(mpa) << endl;
  if (input_record.values.size() > 9 ) {
    if (input_record.values.size() % 2 == 0) {
      throw invalid_argument("Incomplete executions data");
    }
    auto & executions = input_range.rbegin()->executions;
    //ostringstream ss;
    for (size_t j = 9; j < input_record.values.size(); j += 2) {
      const Time exec_time = MkTime(input_record.values[j]);
      const int exec_qty = stoi(input_record.values[j+1]);
      executions.emplace_back(exec_time, exec_qty);
      //ss << " exec_time:" << exec_time << " exec_qty:" << exec_qty;
    }
    //cout << ss.str() << endl;
  }
}

void RodExecutionPlan::Execute() {
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
