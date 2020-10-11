#ifndef TICK_ROD_INCLUDED
#define TICK_ROD_INCLUDED

#include <string>
#include <vector>
#include <map>
#include <vector>

#include "taq-double.h"
#include "taq-time.h"
#include "tick-exec.h"

using namespace std;
using namespace Taq;

namespace tick_calc {
namespace ROD {

using Execution = pair<Time, int>;

enum class RestingType { MinusThree, MinusTwo, MinusOne, Zero, PlusOne, PlusTwo, PlusThree, None, Max = None };

struct InputRecord {
  InputRecord(int id, const string& order_id, Time start_time, Time end_time, char side, int ord_qty, double limit_price, RestingType mpa)
    : id(id), order_id(order_id), start_time(start_time), end_time(end_time),
      side(side), ord_qty(ord_qty), limit_price(limit_price), mpa(mpa) {}
  const int id;
  const string order_id;
  const Time start_time;
  const Time end_time;
  const char side;
  const int ord_qty;
  const Double limit_price;
  const RestingType mpa;
  vector<Execution> executions;
};

struct RodSlice {
  RodSlice(Time start_time, Time end_time, int leaves_qty)
    : start_time(start_time), end_time(end_time), leaves_qty(leaves_qty) { }
  const Time start_time;
  const Time end_time;
  const int leaves_qty;
};

class ExecutionUnit : public tick_calc::ExecutionUnit {
public:
  ExecutionUnit(const string& symbol, Date date, bool adjust_time, map<string, InputRecord> input_records)
    : tick_calc::ExecutionUnit(symbol, date, adjust_time), input_records(move(input_records)) {}
  ~ExecutionUnit() {}
  void Execute() override;
  const map<string, InputRecord> input_records;
};

class ExecutionPlan : public tick_calc::ExecutionPlan {
public:
  
    
private:
public:
  ExecutionPlan(const string& name, const FunctionDef& function_def, const Request& request, const ArgList& arg_list)
    : tick_calc::ExecutionPlan(name, function_def, request, arg_list), progress_cnt(0) {}
  void Input(tick_calc::InputRecord& input_record) override;
  void Execute() override;
private:
  using InputRecordRange = map<string, InputRecord>;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
  int progress_cnt;
};

}
}

#endif
