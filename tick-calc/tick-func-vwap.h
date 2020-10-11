#ifndef TICK_VWAP_INCLUDED
#define TICK_VWAP_INCLUDED

#include <string>
#include <vector>
#include <string>

#include "taq-time.h"
#include "taq-double.h"

#include "tick-exec.h"
#include "tick-duration.h"

using namespace std;
using namespace Taq;

namespace tick_calc {
namespace VWAP {

enum class Type {
  Unknown, ByTime, ByPov, ByTicks, ByMarkouts, MaxVal
};

enum class Flavor{
  Unknown, Exchange , TRF, Regular, Block, All, MaxVal
};

struct InputRecord {
  Time start_time;
  vector<Duration> durations;
  Double limit_price;
  char side;
  Flavor flavor;
  int id;
  InputRecord(int id, Time start_time, Time end_time, char side, Double limit_price, Flavor flavor)
    : start_time(start_time), limit_price(limit_price), side(side), flavor(flavor), id(id) {
    durations.emplace_back(end_time);
  }
  InputRecord(int id, Time start_time, const DurationEnding::Pov & pov, char side, double limit_price, Flavor flavor)
    : start_time(start_time), limit_price(limit_price), side(side), flavor(flavor), id(id) {
    durations.emplace_back(pov);
  }
  InputRecord(int id, Time start_time, int ticks, char side, double limit_price, Flavor flavor)
    : start_time(start_time), limit_price(limit_price), side(side), flavor(flavor), id(id) {
    durations.emplace_back(ticks);
  }
  InputRecord(int id, Time start_time, vector<Duration> & durations, char side, double limit_price, Flavor flavor)
    : start_time(start_time), durations(move(durations)),
    limit_price(limit_price), side(side), flavor(flavor), id(id) {}

  InputRecord(const InputRecord& rhs) = default;
  InputRecord(InputRecord&& rhs) = default;
  InputRecord& operator = (const InputRecord& rhs) = default;
};

class ExecutionUnit : public tick_calc::ExecutionUnit {
public:
  ExecutionUnit(const string& symbol, Date date, bool adjust_time, bool input_sorted, vector<InputRecord> &input_records)
    : tick_calc::ExecutionUnit(symbol, date, adjust_time), input_sorted(input_sorted), input_records(move(input_records)) {}
  ~ExecutionUnit() {}
  void Execute() override;
  const bool input_sorted;
  vector<InputRecord> input_records;
};

class ExecutionPlan : public tick_calc::ExecutionPlan {
private:
public:
  ExecutionPlan(const string& name, const FunctionDef& function_def, const Request& request, const ArgList& arg_list);
  void Input(tick_calc::InputRecord& input_record) override;
  void Execute() override;

private:
  using InputRecordRange = vector<InputRecord>;
  const bool maybe_by_time;
  const bool maybe_by_pov;
  const bool maybe_by_ticks;
  const bool maybe_by_markouts;
  Type type;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
};

struct Result {
  Result() : trade_count(0), trade_volume(0), vwap(0.0) {}
  uint64_t trade_count;
  uint64_t trade_volume;
  double vwap;
};

class  Calculator {
public:
  using Trades = SortedConstVector<Trade>;
  using TradeFilter = function<bool(const Trade& trd, char side, Double price)>;

  Calculator(const Trades& trades, Time start_time, const Duration& duration, char side, Double limit_price, Flavor flavor);
  Result Calculate(const Trade* it);

  const Trades& trades;
  const Time start_time;
  const Duration& duration;
  const char side;
  const Double limit_price;
  const Flavor flavor;
private:
  void Apply(const Trade& trade);
  void CalculateByTime(const Trade* it);
  void CalculateByPov(const Trade* it);
  void CalculateByTick(const Trade* it);
  TradeFilter filter;
  Result result;
};

}
}

#endif
