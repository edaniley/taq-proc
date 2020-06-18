#ifndef TICK_CALC_INCLUDED
#define TICK_CALC_INCLUDED

#include <string>
#include <vector>


#include "taq-proc.h"
#include "tick-data.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

struct AppAruments {
  string in_data_dir;
  string in_cpu_list;
  uint16_t in_port;
};

void NetInitialize(AppAruments&);
void NetPoll(AppAruments&);
void NetFinalize(AppAruments&);

}
#endif

