
#ifdef _MSC_VER
#pragma comment(lib, "jsoncpp.lib")
#endif
#include <iostream>
#include <boost/algorithm/string.hpp>
#include "tick-calc.h"
#include "tick-conn.h"
#include "tick-data.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

void ParseRequest(Connection& conn) {
  JSONCPP_STRING err;
  Json::CharReaderBuilder builder;
  const unique_ptr<Json::CharReader> reader(builder.newCharReader());
  const string json_text =  conn.request_buffer.str();
  cout << json_text << endl;
  if (reader->parse(json_text.c_str(), json_text.c_str() + json_text.size(), &conn.request, &err)) {
    cout << conn.request;
    conn.request_parsed = true;
    // Json::Value & req = conn.request;
    //const string cmd = req["cmd"].asString();
    //const string date = req["date"].asString();
    //const string symbol = req["symbol"].asString();
    //cout << "cmd:" << cmd << " date:" << date << " symbol:" << symbol << endl;
  } else {
    cerr << err << endl;
  }
}

bool HandleInput(Connection & conn) {
  string line = conn.input_buffer.ReadLine();
  while (line.size()) {
    if (!conn.request_parsed) {
      conn.request_buffer << line;
      ParseRequest(conn);
    } else {
      cout << line << endl;
    }
    line = conn.input_buffer.ReadLine();
  }
  conn.input_buffer.FinishReading();
  return true;
}

}
