#ifndef TICK_NBBO_INCLUDED
#define TICK_NBBO_INCLUDED

#include <string>
#include <vector>
#include <map>
#include <vector>

#include "taq-time.h"
#include "tick-exec.h"
#include "tick-duration.h"

using namespace std;
using namespace Taq;

namespace tick_calc {
namespace NBBO {

enum class Type {
  Unknown, PriceAndQty, PriceOnly, MaxVal
};

struct InputRecord {
  InputRecord(int id, Time time) : time(time), id(id) {}
  InputRecord(int id, Time time, vector<Duration>& durations) : time(time), id(id), durations(move(durations)) {}
  Time time;
  int id;
  vector<Duration> durations;
};

class PriceQuantityExecutionUnit : public tick_calc::ExecutionUnit {
public:
  PriceQuantityExecutionUnit(const string& symbol, Date date, bool adjust_time, bool input_sorted, vector<InputRecord> input_records)
    : tick_calc::ExecutionUnit(symbol, date, adjust_time), input_sorted(input_sorted), input_records(move(input_records)) {}
  ~PriceQuantityExecutionUnit() {}
  void Execute() override;
  const bool input_sorted;
  vector<InputRecord> input_records;
};

class PriceOnlyExecutionUnit : public tick_calc::ExecutionUnit {
public:
  PriceOnlyExecutionUnit(const string& symbol, Date date, bool adjust_time, bool input_sorted, vector<InputRecord> input_records)
    : tick_calc::ExecutionUnit(symbol, date, adjust_time), input_sorted(input_sorted), input_records(move(input_records)) {}
  ~PriceOnlyExecutionUnit() {}
  void Execute() override;
  const bool input_sorted;
  vector<InputRecord> input_records;
};

class ExecutionPlan : public tick_calc::ExecutionPlan {
public:
  ExecutionPlan(const string& name, const FunctionDef& function_def, const Request& request, const ArgList& arg_list, Type type);
  void Input(tick_calc::InputRecord& input_record) override;
  void Execute() override;
  void SetResultFields() override;
private:
  const Type type;
  const bool with_markouts;
  using InputRecordRange = vector<InputRecord>;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
};

}
}

#endif
