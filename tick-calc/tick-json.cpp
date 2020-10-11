
#include <sstream>
#include "tick-calc.h"
#include "tick-json.h"

using namespace std;
using namespace Taq;
using namespace tick_calc;

static string RemoveWhiteChars(const string & str) {
  string retval(str);
  retval.erase(
    remove_if(retval.begin(), retval.end(), [](char c) {
      static const string unwanted_chars = " \t\n";
      return unwanted_chars.find(c) != string::npos;
    }),
    retval.end());
  return retval;
}

Json StringToJson(const string & str) {
  Log(LogLevel::INFO, RemoveWhiteChars(str));
  Json json;
  stringstream ss;
  ss << str;
  try {
    boost::property_tree::read_json(ss, json);
  }
  catch (const exception& ex) {
    throw domain_error(string("Inbound json parsing failure:") + ex.what());
  }
  return json;
}

string JsonToString(const Json & json) {
  stringstream ss;
  boost::property_tree::write_json(ss, json);
  string str(RemoveWhiteChars(ss.str()));
  Log(LogLevel::INFO, str);
  str.append("\n");
  return str;
}

string JsonRemoveWhiteChars(const string & str) {
  string retval(str);
  retval.erase(remove_if(retval.begin(), retval.end(), [] (char c) {
    static const string unwanted_chars = " \t\n";
    return unwanted_chars.find(c) != string::npos;
    }), retval.end());
  return retval;
}