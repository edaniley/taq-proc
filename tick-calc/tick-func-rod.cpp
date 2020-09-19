#include <tuple>
#include <algorithm>

#include <boost/algorithm/string.hpp>
#include "taq-proc.h"
#include "taq-text.h"
#include "tick-func-rod.h"

using namespace std;
using namespace Taq;

namespace tick_calc {
namespace ROD {

static const bool registered = RegisterFunctionDefinition(make_unique<FunctionDefinition>("ROD",
  vector<string> {"ID", "Symbol", "Date", "StartTime", "EndTime", "Side", "OrdQty", "LimitPx", "MPA", "ExecTime", "ExecQty"},
  vector<string> {"ID", "MinusThree", "MinusTwo", "MinusOne", "Zero", "PlusOne", "PlusTwo", "PlusThree"})
  );

static char DecodeSide(const string_view& str) {
  if (str.size() > 0) {
    if (str[0] == 'B' || str[0] == 'b')
      return 'B';
    else if (str[0] == 'S' || str[0] == 's')
      return 'S';
  }
  // dead code for now
  string side(str.data(), str.size());
  boost::to_upper(side);
  if (side == "BUY")
    return 'B';
  else if (side == "SELL" || side == "SHORT" || side == "SS" || side == "SSE")
    return 'S';
  throw Exception(ErrorType::InvalidSide);
  return '\0';
}

static bool LooksLikeNumber(const string_view & str) {
  return false == str.empty() && str.compare("nan");
}

static RestingType DecodeRestType(const string_view& str) {
  if (LooksLikeNumber(str)) {
    const int val = TextToNumeric<int>(str);
    if (val >= -(int)RestingType::Zero && val <= (int)RestingType::Zero) {
      return (RestingType)(val + (int)RestingType::Zero);
    }
  }
  return RestingType::None;
}

static RestingType InvertRestType(const RestingType mpa) {
  return (RestingType)(-1 * ((int)mpa - (int)RestingType::Zero) + (int)RestingType::Zero);
}

static RestingType DetermineRestingType(const NbboPrice &nbbo, char side, const Double &limit_price, const RestingType & mpa) {
  if (limit_price.Empty() || limit_price.IsZero() || mpa == RestingType::MinusThree) {
    return mpa;
  }
  RestingType retval = RestingType::None;
  const bool valid_nbbo = nbbo.bidp > 0 && nbbo.askp < numeric_limits<double>::max();
  if (valid_nbbo) {
    const Double bid(nbbo.bidp);
    const Double offer(nbbo.askp);
    const Double mid(.5 * (nbbo.bidp + nbbo.askp));
    if (limit_price.Less(bid)) {
      retval = RestingType::MinusThree;
    } else if (limit_price.Equal(bid)) {
      retval = RestingType::MinusTwo;
    } else if (limit_price.Greater(bid) && limit_price.Less(mid)) {
      retval = RestingType::MinusOne;
    } else if (limit_price.Equal(mid)) {
      retval = RestingType::Zero;
    } else if (limit_price.Greater(mid) && limit_price.Less(offer)) {
      retval = RestingType::PlusOne;
    } else if (limit_price.Equal(offer)) {
      retval = RestingType::PlusTwo;
    } else {
      retval = RestingType::PlusThree;
    }
    if (mpa == RestingType::PlusThree) {
      retval = min(retval, RestingType::Zero);
    } else if (mpa != RestingType::None) {
      retval = min(retval, mpa);
    }
    retval = side == 'B' ? retval : InvertRestType(retval);
  }
  return retval;
}

static void CalculateROD(vector<double> &result, const NbboPrice*quote_start, const NbboPrice* quote_end,
                        const vector<RodSlice> slices, char side, const Double &limit_price, const RestingType & mpa) {
    auto slice = slices.begin();
    const NbboPrice* current_quote = quote_start;
    while (slice != slices.end()) {
      const Time start_time = max(slice->start_time, current_quote->time); // latest of slice start time or curent nbbo time
      const NbboPrice* next_quote = (current_quote + 1) < quote_end ? current_quote + 1 : nullptr;
      const Time end_time = next_quote            // check if subsequent quote is present
        ? min(slice->end_time, next_quote->time)  // earlierst of end of slice or nbbo change i.e. next nbbo time
        : slice->end_time;

      const RestingType rest_type = DetermineRestingType(*current_quote, side, limit_price, mpa);
      if (rest_type != RestingType::None) {
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

void ExecutionUnit::Execute() {
  auto& secmaster_mgr = SecurityMasterManager();
  auto& quote_mgr = NbboPoRecordsetManager();
  const SecMaster* secmaster = nullptr;
  const SymbolRecordset<NbboPrice> * symbol_recordset = nullptr;
  try {
    secmaster = &secmaster_mgr.Load(date);
    const Security& security = secmaster->FindBySymbol(symbol);
    symbol_recordset = &quote_mgr.LoadSymbolRecordset(date, security.symb);
  } catch (...) {
    Error(ErrorType::DataNotFound, (int)input_records.size());
    return;
  }
  auto & quotes = symbol_recordset->records;
  auto quote_start = quotes.begin();
  vector<const InputRecord *> sorted_input(input_records.size());
  size_t j = 0;
  for (auto it = input_records.begin(); it != input_records.end(); ++ it) {
    sorted_input[j++] = &it->second;
  }
  sort(sorted_input.begin(), sorted_input.end(), [](const InputRecord *lh, const InputRecord *rh) {
    return lh->start_time < rh->start_time;
  });
  for (const auto prec : sorted_input) {
    const InputRecord &rec = *prec;
    try {
      ostringstream ss;
      const Time start_time_adjusted = rec.start_time + taq_time_adjustment;
      const Time end_time_adjusted = rec.end_time + taq_time_adjustment;

      quote_start = quotes.find_prior(quote_start, quotes.end(), start_time_adjusted);
      if (quote_start == quotes.end()) {
        throw Exception(ErrorType::DataNotFound, "Market data not found");
      }
      vector<RodSlice> slices;
      if (rec.executions.empty()) {
        // no executions
        if (start_time_adjusted < end_time_adjusted) {
          slices.emplace_back(start_time_adjusted, end_time_adjusted, rec.ord_qty);
        }
      } else {
        // sort by exec time
        vector<pair<Time, int>> sorted_executions(rec.executions.size());
        copy(rec.executions.begin(), rec.executions.end(), sorted_executions.begin());
        sort(sorted_executions.begin(), sorted_executions.end(), [](const Execution& lh, const Execution& rh) {
          return lh.first < rh.first;
        });
        // split order duration into execution count + 1 slices (start and end times, and leaves qty)
        int leaves_qty = rec.ord_qty;
        Time slice_start_time = start_time_adjusted;
        Time slice_end_time;
        for (auto exec : sorted_executions) {
          const Time exec_time_adjusted = exec.first + taq_time_adjustment;
          const int exec_qty = exec.second;
          slice_end_time = exec_time_adjusted;
          if (slice_end_time < slice_start_time) {
            throw Exception(ErrorType::InvalidTimestamp, "Out of sequence timestamp");
          }
          else if (slice_end_time > slice_start_time) {
            slices.emplace_back(slice_start_time, slice_end_time, leaves_qty);
            slice_start_time = slice_end_time;
          }
          leaves_qty -= exec_qty;
        }
        if (leaves_qty < 0) {
          throw Exception(ErrorType::InvalidQuantity, "Sum of execution quantity exceeds order quantity");
        } else if (leaves_qty > 0 and slice_start_time < end_time_adjusted) {
          // create final slice with time remaining and non-zero LeavesQty
          slices.emplace_back(slice_start_time, end_time_adjusted, leaves_qty);
        }
      }

      // calculate rod here
      vector<double> rod_values((size_t)RestingType::Max, .0);
      if (!slices.empty()) {
        auto quote_end = quotes.upper_bound(quote_start, quotes.end(), slices.rbegin()->end_time);
        CalculateROD(rod_values, quote_start, quote_end, slices, rec.side, rec.limit_price, rec.mpa);
      }
      ss << rec.order_id;
      for (size_t i = 0; i < rod_values.size(); i ++) {
        ss << '|' << rod_values[i];
      }
      ss << endl;
      output_records.emplace_back(rec.id, ss.str());
    } catch (Exception & Ex) {
      Error(Ex.errtype());
    } catch (exception & ex) {
      cerr << ex.what() << endl; // log file
    }
  }

  quote_mgr.UnloadSymbolRecordset(date, symbol);
  secmaster_mgr.Release(*secmaster);
}

void ExecutionPlan::Input(tick_calc::InputRecord& input_record) {
  enum args {ID, SYMBOL, DATE, START_TIME, END_TIME, SIDE, ORD_QTY, LMT_PX, MPA, EXEC_TIME, EXEC_QTY};
  try {
    if (input_record.values[SYMBOL].empty())
      throw Exception(ErrorType::MissingSymbol);

    const string id(input_record.values[ID].data(), input_record.values[ID].size());
    const auto & symbol = input_record.values[SYMBOL];
    const Date date = MkDate(input_record.values[DATE]);
    InputRecordRange& input_range = input_record_ranges[make_pair(string(symbol.data(), symbol.size()), date)];
    InputRecord* rec = nullptr;
    auto it = input_range.find(id);
    if (it == input_range.end()) {
      const Time start_time = MkTime(input_record.values[START_TIME]);
      const Time end_time = MkTime(input_record.values[END_TIME]);
      const char side = DecodeSide(input_record.values[SIDE]);
      const int ord_qty = TextToNumeric<int>(input_record.values[ORD_QTY]);
      const Double limit_price(input_record.values[LMT_PX]);
      const RestingType mpa = DecodeRestType(input_record.values[MPA]);
      auto pit = input_range.try_emplace(id, input_record.id, id, start_time, end_time, side, ord_qty, limit_price, mpa);
      rec = &(pit.first->second);
    } else {
      rec = &(it->second);
    }
    const auto & qty = input_record.values[EXEC_QTY];
    if (LooksLikeNumber(qty)) {
      const Time exec_time = MkTime(input_record.values[EXEC_TIME]);
      const int exec_qty = TextToNumeric<int>(qty);
      rec->executions.emplace_back(exec_time, exec_qty);
    }
  }
  catch (const Exception & Ex) {
    Error(Ex.errtype());
  }
  catch (...) {
    Error(ErrorType::InvalidArgument);
  }
  //if (IsVerbose()) {
  //  record_cnt ++;
  //  if (record_cnt % 1000 == 0) {
  //    if (++progress_cnt == 100) {
  //      cout << "." << record_cnt << endl;
  //      progress_cnt = 0;
  //    } else cout << ".";
  //  }
  //}
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
      get<0>(slice), get<1>(slice), request.tz_name == "UTC", move(*get<2>(slice))
    );
    todo_list.push_back(job);
    AddExecutionUnit(job);
  }
}

}
}
