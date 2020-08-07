#ifndef TICK_REQUEST_INCLUDED
#define TICK_REQUEST_INCLUDED

#include <string>
#include <vector>
#include <map>

#include "taq-proc.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

struct Request {
  Request() : input_cnt(0) {}
  string id;
  string separator;
  string tz_name;
  string output_format;
  vector<string> function_list;
  vector<string> argument_list;
  map<string, vector<int>> functions_argument_mapping;
  bool input_sorted;
  int input_cnt;
};

}

#endif

