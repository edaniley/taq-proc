#ifndef TICK_REQUEST_INCLUDED
#define TICK_REQUEST_INCLUDED

#include <string>
#include <vector>

#include "tick-json.h"

using namespace std;

namespace tick_calc {

using ArgPos = pair<string, int>; // argument name and its position in input record (1-based)
using ArgList = vector<ArgPos>;

struct FunctionArgs {
  FunctionArgs(const string & function_name, const string& alias, ArgList & arguments)
    : function_name(function_name), alias(alias), arguments(move(arguments)) {}
  const string function_name;
  const string alias;
  const ArgList arguments;
};

struct Request {
  Request() : input_sorted(false), input_cnt(0) {}
  string id;
  string tz_name;
  bool input_sorted;
  int input_cnt;
  vector<FunctionArgs> input_arguments;
};


vector<FunctionArgs> ReadArgumentList(const Json &);

}

#endif

