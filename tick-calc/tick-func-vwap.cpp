#include <tuple>
#include <algorithm>
#include <iterator>
#include <cmath>

#include <boost/algorithm/string.hpp>
#include "taq-proc.h"
#include "tick-calc.h"
#include "tick-func.h"
#include "double.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

class VwapFunction : public  FunctionDefinition {
public:
  VwapFunction(const string& name, const vector<string>& argument_names, const vector<string>& output_fields) :
    FunctionDefinition(name, argument_names, output_fields) {}
  void ValidateArgumentList(const vector<int>& argument_mapping) const override {
    enum ArgumentNo { Symbol, Date, StartTime, Side, LimitPx, Flavor, EndTime, POV, Ticks };
    bool valid = argument_mapping[Symbol] != -1 && argument_mapping[Date] != -1 && argument_mapping[StartTime] != -1;
    valid = valid && (argument_mapping[EndTime] != -1 || argument_mapping[POV] != -1 || argument_mapping[Ticks] != -1);
    if (!valid) {
      throw invalid_argument("Invalid argument list");
    }
  }
};

static const bool registered = RegisterFunctionDefinition(make_unique<VwapFunction>("VWAP",
    vector<string> {"Symbol", "Date", "StartTime", "Side", "LimitPx", "Flavor", "EndTime", "POV", "Ticks"},
    vector<string> {"ID", "TradeCnt", "TradeVolume", "VWAP"})
    );

using VwapType = VwapExecutionPlan::VwapType;

void VwapExecutionPlan::VwapExecutionUnit::Execute() {
}
void VwapExecutionPlan::Input(InputRecord & input_record) {
}
void VwapExecutionPlan::Execute() {
}
}
