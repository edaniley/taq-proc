#ifndef TICK_EXEC_INCLUDED
#define TICK_EXEC_INCLUDED

#include <string>
#include <vector>
#include <atomic>
#include <functional>

#include "taq-proc.h"
#include "tick-data.h"
#include "tick-conn.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

struct FunctionDefinition {
  FunctionDefinition(const vector<string> & argument_names, const vector<string> & result_field_names)
    : argument_names(argument_names), result_field_names(result_field_names) {}
  const vector<string> argument_names;
  const vector<string> result_field_names;
};


class ExecutionPlan {
public:
  ExecutionPlan(const vector<int>& argument_mapping, bool sorted_input = true) :
    argument_mapping(argument_mapping), sorted_input(sorted_input) {}
  virtual ~ExecutionPlan() {};
  virtual void Input(const Record&) = 0;
  virtual void Execute() = 0;
protected:
  bool sorted_input;
  vector<int> argument_mapping;
};


struct ExecutionUnit {
  ExecutionUnit() {
    ready.store(false);
  }
  typedef vector<string> ValueSet;
  function<void(const Record &)> init;
  function<void(const Record &)> exec;
  vector<Record> input_recordset;
  vector<Record> result_recordset;
  atomic<bool> ready;
};

}

#endif
