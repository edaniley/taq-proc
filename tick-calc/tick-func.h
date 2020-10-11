#ifndef TICK_FUNC_INCLUDED
#define TICK_FUNC_INCLUDED

#include <map>
#include "taq-proc.h"
#include "tick-data.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

using SymbolDateKey = pair<string, Date>;

struct FieldDef {
  FieldDef(const string& name, const string& type, size_t size) : name(name), type(data_type(type, size)) {}
  FieldDef(const string& name, const string& type) : name(name), type(type) {}
  const string name;
  const string type;
  static string data_type(const string& type, size_t size) {
    if (type == typeid(char).name()) {
      ostringstream ss;
      ss << 'a' << size;
      return ss.str();
    } else if (type == typeid(double).name()) {
      return "double";
    } else if (type == typeid(int).name()) {
      return "int";
    }
    return type;
  }
};

class FunctionDef{
public:
  FunctionDef(const string& name, const vector<string>& argument_names, const vector<FieldDef>& output_fields)
    : name(name), argument_names(argument_names), output_fields(output_fields) { }
  virtual ~FunctionDef() {}

  virtual void ValidateArgumentList(const vector<int>& argument_mapping) const {
    for (size_t i = 0; i < argument_names.size(); i++) {
      if (argument_mapping[i] == -1) { // by default presence of all arguments is expected
        throw invalid_argument("Missing argument:" + argument_names[i] + " function:" + name);
      }
    }
  }

  const string name;
  const vector<string> argument_names;
  const vector<FieldDef> output_fields;
};

inline bool IsArgumentPresent(const vector<int>& arg_map, int f) {
  return f < (int)arg_map.size() && arg_map[f] != -1;
}

bool RegisterFunctionDefinition(unique_ptr<FunctionDef>);
const FunctionDef& FindFunctionDefinition(const string& function_name);

}
#endif
