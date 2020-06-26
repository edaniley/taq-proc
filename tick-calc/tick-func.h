#ifndef TICK_FUNC_INCLUDED
#define TICK_FUNC_INCLUDED

#include <map>
#include "taq-proc.h"
#include "tick-data.h"
#include "tick-exec.h"
#include "double.h"

using namespace std;
using namespace Taq;
using namespace taq_proc;

namespace tick_calc {

using SymbolDateKey = pair<string, Date>;

class QuoteExecutionPlan : public ExecutionPlan {
  class QuoteExecutionUnit : public ExecutionUnit {
  public:
    struct InputRecord {
      InputRecord(int id, Time time) : time(time), id(id) {}
      const Time time;
      const int id;
    };
    QuoteExecutionUnit(const string& symbol, Date date, vector<InputRecord> input_records)
      : symbol(symbol), date(date), input_records(move(input_records)) {}
    ~QuoteExecutionUnit() {}
    void Execute() override;
    const string symbol;
    const Date date;
    const vector<InputRecord> input_records;
  };
public:
  QuoteExecutionPlan(const FunctionDefinition& function, const Request& request, const vector<int>& argument_mapping)
    : ExecutionPlan(function, request, argument_mapping) {}
  void Input(InputRecord& input_record) override;
  void Execute() override;
private:
  using InputRecordRange = vector<QuoteExecutionUnit::InputRecord>;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
};

class RodExecutionPlan : public ExecutionPlan {
public:
  enum class RestType { MinusThree, MinusTwo, MinusOne, Zero, PlusOne, PlusTwo, PlusThree, None, Max = None };
private:
  class RodExecutionUnit : public ExecutionUnit {
  public:
    struct InputRecord {
      InputRecord(int id, const string &order_id, Time start_time, Time end_time, char side, int ord_qty,
                  double limit_price, RodExecutionPlan::RestType mpa)
        : id(id), order_id(order_id), start_time(start_time), end_time(end_time),
          side(side), ord_qty(ord_qty), limit_price(limit_price), mpa(mpa) {}
        const int id;
        const string order_id;
        const Time start_time;
        const Time end_time;
        const char side;
        const int ord_qty;
        const Double limit_price;
        const RodExecutionPlan::RestType mpa;
        vector<pair<Time, int>> executions;
    };
    RodExecutionUnit(const string& symbol, Date date, vector<InputRecord> input_records)
      : symbol(symbol), date(date), input_records(move(input_records)) {}
    ~RodExecutionUnit() {}
    void Execute() override;
    const string symbol;
    const Date date;
    const vector<InputRecord> input_records;
  };
public:
  RodExecutionPlan(const FunctionDefinition& function, const Request& request, const vector<int>& argument_mapping)
    : ExecutionPlan(function, request, argument_mapping), progress_cnt(0) {}
  void Input(InputRecord& input_record) override;
  void Execute() override;
private:
  using InputRecordRange = vector<RodExecutionUnit::InputRecord>;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
  int progress_cnt;
};

}
#endif


