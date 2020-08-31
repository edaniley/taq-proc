#ifndef TICK_VWAP_INCLUDED
#define TICK_VWAP_INCLUDED

#include "tick-func.h"


namespace tick_calc {

enum class VwapDurationType {
  Unknown, ByTime = 1 , ByPov = 2, ByTicks = 3, RecordSpecific = 4, MaxVal
};

enum class VwapFlavor{
  Unknown, Exchange , TRF, Regular, All, MaxVal
};

union VwapDuration {
  VwapDuration(const VwapDuration & copy) { pov = copy.pov; }
  VwapDuration(VwapDuration&& obj) { pov = obj.pov; }
  VwapDuration(Time end_time) : end_time(end_time) {}
  VwapDuration(pair<size_t, double> pov) : pov(pov) {}
  VwapDuration(int ticks) : ticks(ticks) {}
  VwapDuration & operator = (const VwapDuration & obj) { pov = obj.pov; return *this; }
  Time end_time;
  pair<uint64_t, double> pov;// target volume and participation
  int ticks;
};

struct VwapEndTime {
  VwapEndTime(Time end_time) : duration(end_time) , type(VwapDurationType::ByTime) {}
  VwapEndTime(pair<uint64_t, double> pov) : duration(pov), type(VwapDurationType::ByPov) {}
  VwapEndTime(int ticks) : duration(ticks) , type(VwapDurationType::ByTicks) {}
  VwapEndTime & operator = (const VwapEndTime &rhs) {
    duration = rhs.duration;
    type = rhs.type;
    return *this;
  }
  VwapDuration duration;
  VwapDurationType type;
};

struct VwapInputRecord {
  Time start_time;
  VwapEndTime end_time;
  Double limit_price;
  char side;
  uint8_t flavor;
  int id;
  VwapInputRecord(int id, Time start_time, Time end_time, char side, Double limit_price, VwapFlavor flavor)
    : start_time(start_time), end_time(end_time),
      side(side), limit_price(limit_price), flavor((uint8_t)flavor), id(id) {}
  VwapInputRecord(int id, Time start_time, pair<uint64_t, double> pov, char side, double limit_price, VwapFlavor flavor)
    : start_time(start_time), end_time(pov),
      side(side), limit_price(limit_price), flavor((uint8_t)flavor), id(id) {}
  VwapInputRecord(int id, Time start_time, int ticks, char side, double limit_price, VwapFlavor flavor)
    : start_time(start_time), end_time(ticks),
      side(side), limit_price(limit_price), flavor((uint8_t)flavor), id(id) {}
  VwapInputRecord(const VwapInputRecord& rhs) = default;
  VwapInputRecord(VwapInputRecord&& rhs) = default;
  VwapInputRecord & operator = (const VwapInputRecord & rhs) {
    start_time = rhs.start_time;
    end_time = rhs.end_time;
    limit_price = rhs.limit_price;
    side = rhs.side;
    flavor = rhs.flavor;
    id = rhs.id;
    return *this;
  }
};

class VwapExecutionUnit : public ExecutionUnit {
public:
  VwapExecutionUnit(const string& symbol, Date date, bool input_sorted, bool adjust_time, vector<VwapInputRecord> &input_records)
    : symbol(symbol), date(date), input_sorted(input_sorted), adjust_time(adjust_time), input_records(move(input_records)) {}
  ~VwapExecutionUnit() {}
  void Execute() override;
  const string symbol;
  const Date date;
  const bool input_sorted;
  const bool adjust_time;
  vector<VwapInputRecord> input_records;
};

class VwapExecutionPlan : public ExecutionPlan {
private:
public:
  VwapExecutionPlan(const FunctionDefinition& function, const Request& request, const vector<int>& argument_mapping);
  void Input(InputRecord& input_record) override;
  void Execute() override;
private:
  using InputRecordRange = vector<VwapInputRecord>;
  void InputByTime(const InputRecord& input_record, InputRecordRange& input_range) const;
  void InputByPov(const InputRecord& input_record, InputRecordRange& input_range) const;
  void InputByTicks(const InputRecord& input_record, InputRecordRange& input_range) const;
  const bool maybe_by_time;
  const bool maybe_by_pov;
  const bool maybe_by_ticks;
  VwapDurationType duration_type;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
};

}

#endif
