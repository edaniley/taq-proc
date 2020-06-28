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
//static mutex cout_mtx;
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

static char DecodeSide(const string& str) {
  if (str.size() == 1) {
    if (str[0] == 'B' || str[0] == 'b')
      return 'B';
    else if (str[0] == 'S' || str[0] == 's')
      return 'S';
  }
  string side(str);
  boost::to_upper(side);
  if (side == "BUY")
    return 'B';
  else if (side == "SS" || side == "SSE" || side == "SELL")
    return 'S';
  throw Exception(ErrorType::InvalidSide);
  return '\0';
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
  // auto started = pt::microsec_clock::local_time();
  // {
  //   unique_lock<mutex> lock(cout_mtx);
  //   cout << started << " thread-id:" << this_thread::get_id() << " started symbol:" << symbol
  //        << " record-cnt:" << input_records.size() << endl;
  // }
  auto& quote_mgr = NbboPoRecordsetManager();
  const SymbolRecordset<NbboPrice> * symbol_recordset = nullptr;
  try {
    symbol_recordset = &quote_mgr.LoadSymbolRecordset(date, symbol);
  } catch (const exception & ex) {
    // unique_lock<mutex> lock(cout_mtx);
    // cout << started << " thread-id:" << this_thread::get_id() << " error:" << ex.what() << endl;
    Error(ErrorType::DataNotFound, (int)input_records.size());
    return;
  }
  auto & quotes = symbol_recordset->records;
  auto quote_start = quotes.begin();
  for (auto rec : input_records) {
    ostringstream ss;
    try {
      quote_start = quotes.find_prior(quote_start, quotes.end(), rec.start_time);
      if (quote_start == quotes.end()) {
        throw Exception(ErrorType::DataNotFound, "Market data not found");
      }
      vector<RodSlice> slices;
      if (rec.executions.empty()) {
        // no executions
        if (rec.start_time < rec.end_time) {
          slices.emplace_back(rec.start_time, rec.end_time, rec.ord_qty);
        }
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
            throw Exception(ErrorType::InvalidTimestamp, "Out of sequence timestamp");
          }
          else if (end_time > start_time) {
            slices.emplace_back(start_time, end_time, leaves_qty);
            start_time = end_time;
          }
          leaves_qty -= exec_qty;
        }
        if (leaves_qty < 0) {
          throw Exception(ErrorType::InvalidQuantity, "Sum of execution quantity exceeds order quantity");
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
      vector<double> rod_values((size_t)RestType::Max, .0);
      if (!slices.empty()) {
        auto quote_end = quotes.upper_bound(quote_start, quotes.end(), slices.rbegin()->end_time);
        CalculateROD(rod_values, quote_start, quote_end, slices, rec.side, rec.limit_price, rec.mpa);
      }
      ss << rec.id;
      for (size_t i = 0; i < rod_values.size(); i ++) {
        ss << '|' << rod_values[i];
      }
      ss << endl;
    } catch (Exception & Ex) {
      Error(Ex.errtype());
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
  // auto finished = pt::microsec_clock::local_time();
  // {
  //   unique_lock<mutex> lock(cout_mtx);
  //   cout << started << " thread-id:" << this_thread::get_id() << " finished symbol:" << symbol
  //     << " run-time:" << (finished - started) << endl;
  // }
}

void RodExecutionPlan::Input(InputRecord& input_record) {
  enum args {ID, SYMBOL, DATE, START_TIME, END_TIME, SIDE, ORD_QTY, LMT_PX, MPA, EXEC_BASE};
  try {
    if (input_record.values[SYMBOL].empty())
      throw Exception(ErrorType::MissingSymbol);

    const string& id= input_record.values[ID];
    const string& symbol = input_record.values[SYMBOL];
    const Date date = MkDate(input_record.values[DATE]);
    const Time start_time = MkTime(input_record.values[START_TIME]);
    const Time end_time = MkTime(input_record.values[END_TIME]);
    const char side = DecodeSide(input_record.values[SIDE]);
    const int ord_qty = stoi(input_record.values[ORD_QTY]);
    const Double limit_price(input_record.values[LMT_PX]);
    const RestType mpa = DecodeRestType(input_record.values[MPA]);
    InputRecordRange& input_range = input_record_ranges[make_pair(symbol, date)];
    input_range.emplace_back(input_record.id, id, start_time, end_time, side, ord_qty, limit_price, mpa);
    if (input_record.values.size() > EXEC_BASE) {
      if (input_record.values.size() % 2 == 0) {
        throw Exception(ErrorType::InvalidArgument);
      }
      auto & executions = input_range.rbegin()->executions;
      for (size_t j = EXEC_BASE; j < input_record.values.size(); j += 2) {
        if (input_record.values[j + 1].empty())
          throw Exception(ErrorType::InvalidQuantity);
        const Time exec_time = MkTime(input_record.values[j]);
        const int exec_qty = stoi(input_record.values[j+1]);
        executions.emplace_back(exec_time, exec_qty);
      }
    }
  }
  catch (const Exception & Ex) {
    Error(Ex.errtype());
  }
  catch (...) {
    Error(ErrorType::InvalidArgument);
  }
  record_cnt ++;
  if (record_cnt % 1000 == 0) {
    if (++progress_cnt == 100) {
      cout << "." << record_cnt << endl;
      progress_cnt = 0;
    } else cout << ".";
  }
}

void RodExecutionPlan::Execute() {
  typedef tuple<string, Date, InputRecordRange*> InputRecordSlice;
  vector<InputRecordSlice> slices;
  for (auto& range : input_record_ranges) {
    slices.push_back(make_tuple(range.first.first, range.first.second, &range.second));
  }
  sort(slices.begin(), slices.end(), [](const InputRecordSlice& left, const InputRecordSlice& right) {
    return (get<2>(left)->size() > get<2>(right)->size());
    });
  for (auto& slice : slices) {
    shared_ptr<ExecutionUnit> job = make_shared<RodExecutionUnit>(get<0>(slice), get<1>(slice), move(*get<2>(slice)));
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

}
