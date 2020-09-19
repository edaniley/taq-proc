#ifndef TICK_FUNC_INCLUDED
#define TICK_FUNC_INCLUDED

#include <map>
#include "taq-proc.h"
#include "tick-data.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

using SymbolDateKey = pair<string, Date>;

class FunctionDefinition {
public:
  FunctionDefinition(const string& name, const vector<string>& argument_names, const vector<string>& output_fields)
    : name(name), argument_names(argument_names), output_fields(output_fields) { }
  virtual ~FunctionDefinition() {}

  vector<int> MapArgumentList(const vector<string>& request_arguments) const {
    vector<int> argument_mapping(argument_names.size(), -1);
    for (size_t i = 0; i < argument_names.size(); i++) {
      for (size_t j = 0; j < request_arguments.size(); j++) {
        if (request_arguments[j] == argument_names[i]) {
          argument_mapping[i] = (int)j;
          break;
        }
      }
    }
    return argument_mapping;
  }

  virtual void ValidateArgumentList(const vector<int>& argument_mapping) const {
    for (size_t i = 0; i < argument_names.size(); i++) {
      if (argument_mapping[i] == -1) { // by default presence of all arguments is expected
        throw invalid_argument("Missing argument:" + argument_names[i] + " function:" + name);
      }
    }
  }


  const string name;
  const vector<string> argument_names;
  const vector<string> output_fields;
};

inline bool IsArgumentPresent(const vector<int>& arg_map, int f) {
  return f < (int)arg_map.size() && arg_map[f] != -1;
}

bool RegisterFunctionDefinition(unique_ptr<FunctionDefinition>);
const FunctionDefinition& FindFunctionDefinition(const string& function_name);

}
#endif
