#ifndef TICK_CALC_INCLUDED
#define TICK_CALC_INCLUDED

#include <string>
#include <vector>
#include <string_view>

#include "taq-proc.h"
#include "taq-text.h"
#include "tick-data.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

struct AppAruments {
  string in_data_dir;
  string in_cpu_list;
  uint16_t in_port;
  string log_dir;
};

void NetInitialize(AppAruments&);
void NetPoll();
void NetFinalize(AppAruments&);
bool IsVerbose();

enum class LogLevel {
  CRITICAL, ERR, WARN, INFO, DEBUG
};

void LogInitialize(AppAruments&);
void LogFinalize(AppAruments&);
void Log(LogLevel, const string&);
void LogPoll();

}
#endif

