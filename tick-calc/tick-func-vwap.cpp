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

using VwapType = VwapExecutionPlan::VwapType;

void VwapExecutionPlan::VwapExecutionUnit::Execute() {
}
void VwapExecutionPlan::Input(InputRecord & input_record) {
}
void VwapExecutionPlan::Execute() {
}
}
