
#include <sstream>
#include "tick-calc.h"
#include "tick-json.h"

using namespace std;
using namespace Taq;

js::ptree StringToJson(const string& json_str) {
  js::ptree root;
  stringstream ss;
  ss << json_str;
  try {
    js::read_json(ss, root);
    if (tick_calc::IsVerbose()) {
      js::write_json(cout, root);
    }
  }
  catch (const exception& ex) {
    throw domain_error(string("Inbound json parsing failure:") + ex.what());
  }
  return root;
}

string JsonToString(const js::ptree& root) {
  stringstream ss;
  js::write_json(ss, root);
  if (tick_calc::IsVerbose()) {
    cout << ss.str();
  }
  string output = ss.str();
  output.erase(remove_if(output.begin(), output.end(), [](char c) {return c == '\n'; }), output.end());
  replace(output.begin(), output.end(), '\t', ' ');
  output.append("\n");
  return output;
}
