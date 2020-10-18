#include "tick-request.h"

namespace tick_calc {

static pair<string, string> ReadStringPair(const Json & json) {
  vector<string> args;
  for (const auto& it : json) {
    if (it.second.empty()) {
      args.emplace_back(it.second.data());
    }
  }
  return args.size() == 2 ? make_pair(move(args[0]), move(args[1])) : pair<string, string>();
}

vector<FunctionArgs> ReadArgumentList(const Json & json) {
  string str = JsonToString(json);
  vector<FunctionArgs> retval;
  for (const auto& it : json) {
    const string function_name = it.second.get<string>("function");
    const string alias = it.second.get<string>("alias", "");
    ArgList arguments;
    for (const auto& arg : it.second.get_child("arguments")) {
      pair<string, string> arg_pos = ReadStringPair(arg.second);
      arguments.emplace_back(move(arg_pos.first), stoi(arg_pos.second));
    }
    retval.emplace_back(function_name, alias, arguments);
  }
  return retval;
}

}
