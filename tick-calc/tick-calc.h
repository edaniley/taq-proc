#ifndef TICK_CALC_INCLUDED
#define TICK_CALC_INCLUDED

#include <string>
#include <vector>
#include <memory>

#include "taq-proc.h"
#include "tick-data.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

struct AppContext {
  string in_data_dir;
  string in_cpu_list;
  uint16_t in_port;
  vector<int> cpu_cores;
  unique_ptr<RecordsetManager<Nbbo>> nbbo_data_manager;
  unique_ptr<RecordsetManager<Trade>> trade_data_manager;
};

void InitializeFunctionDefinitions();
void NetInitialize(AppContext &);
void NetPoll(AppContext &);

}
#endif

