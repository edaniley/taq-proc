#include <iostream>
#include <exception>
#include <chrono>
#include <cstdlib>
#include <boost/algorithm/string.hpp>
#include "tick-calc.h"
#include "tick-conn.h"
#include "tick-data.h"
#include "tick-func.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

class TestLoadExecutionUnit : public ExecutionUnit {
public:
  TestLoadExecutionUnit(const vector<int>& argument_mapping, InputRecordRange input) : input(input) {
    function_idx = argument_mapping[0];
    symbol_idx = argument_mapping[1];
    date_idx = argument_mapping[2];
  }
  void Execute() override {
    for (auto i = input.first; i < input.end; i++) {
      const InputRecord& input_record = input.records[i];
      OutputRecord output_record(input_record.id);
      const string& function = input_record.values[function_idx];
      const string& symbol = input_record.values[symbol_idx];
      const Date date = MkTaqDate(input_record.values[date_idx]);
      ostringstream ss;
      ss << function << '|' << symbol << '|' << boost::gregorian::to_iso_extended_string(date) << endl;
      output_record.value = ss.str();
      output_records.push_back(move(output_record));
    }
  }
  int function_idx;
  int symbol_idx;
  int date_idx;
  const InputRecordRange input;
};

void TestLoad::Input(InputRecord& input_record) {
  intput_records.push_back(move(input_record));
};
   
void TestLoad::Execute() {
  InputRecordRange input_record_range(intput_records, 0 , (int)intput_records.size());
  shared_ptr<ExecutionUnit> job = make_shared<TestLoadExecutionUnit>(argument_mapping, input_record_range);
  todo_list.push_back(job);
  AddExecutionUnit(job);
}

}
