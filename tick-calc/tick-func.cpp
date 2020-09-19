
#include "tick-exec.h"
#include "tick-func.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

using FunctionRegistry = map <string, unique_ptr<FunctionDefinition>>;
static unique_ptr<FunctionRegistry> function_regisitry;

bool RegisterFunctionDefinition(unique_ptr<FunctionDefinition> ptr) {
  if (!function_regisitry) {
    function_regisitry = make_unique<FunctionRegistry>();
  }
  auto it = function_regisitry->insert(make_pair(ptr->name, move(ptr)));
  (void)it; assert(it.second == true);
  return true;
}

const FunctionDefinition& FindFunctionDefinition(const string& function_name) {
  auto it = function_regisitry->find(function_name);
  if (it == function_regisitry->end()) {
    throw invalid_argument("Unknown function:" + function_name);
  }
  return *it->second;
}

}
