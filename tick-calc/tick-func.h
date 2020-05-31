#ifndef TICK_FUNC_INCLUDED
#define TICK_FUNC_INCLUDED

#include <map>
#include "taq-proc.h"
#include "tick-data.h"
#include "tick-exec.h"


using namespace std;
using namespace Taq;

namespace tick_calc {

class QuoteExecutionPlan : public ExecutionPlan {
  class QuoteExecutionUnit : public ExecutionUnit {
  public:
    struct InputRecord {
      InputRecord(int id, Time time) : time(time), id(id) {}
      const Time time;
      const int id;
    };
    QuoteExecutionUnit(const string& symbol, Date date, vector<InputRecord> input_records)
      : symbol(symbol), date(date), input_records(move(input_records)) {cout << "+ QuoteExecutionUnit" << endl;}
    ~QuoteExecutionUnit() {cout << "- QuoteExecutionUnit" << endl;}
    void Execute() override;
    const string symbol;
    const Date date;
    const vector<InputRecord> input_records;
    //vector <OutputRecord> output_records;
  };
public:
  QuoteExecutionPlan(const vector<int>& argument_mapping, const string& field_separator, bool sorted_input = true)
    : ExecutionPlan(argument_mapping, field_separator, sorted_input) {}
  void Input(InputRecord& input_record) override;
  void Execute() override;
private:
  typedef pair<string, Date> SymbolDateKey;
  typedef vector<QuoteExecutionUnit::InputRecord> InputRecordRange;
  map<SymbolDateKey, InputRecordRange> input_record_ranges;
};


class TestLoad : public ExecutionPlan {
public:
  TestLoad(const vector<int>& argument_mapping, const string& field_separator, bool sorted_input = true)
    : ExecutionPlan(argument_mapping, field_separator, sorted_input) {}
  void Input(InputRecord& input_record) override;
  void Execute() override;
private:
  InputRecordSet intput_records;
};


}
#endif


