
#ifdef _MSC_VER
#pragma comment(lib, "jsoncpp.lib")
#endif
#include <iostream>
#include <exception>
#include <list>
#include <boost/algorithm/string.hpp>
#include "tick-calc.h"
#include "tick-conn.h"
#include "tick-data.h"
#include "tick-exec.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

list<ExecutionUnit> todo_list;
list<ExecutionUnit> done_list;

static map<string, FunctionDefinition> function_definitions;

void InitializeFunctionDefinitions() {
  function_definitions.insert(make_pair("TestLoad", FunctionDefinition(
    vector<string> {"Function", "Symbol", "Date"}, vector<string> {"Result", "ErrMsg"})));

  function_definitions.insert(make_pair("Quotes", FunctionDefinition(
    vector<string> {"Symbol", "Date", "StartTime", "EndTime"},
    vector<string> {"BidPrice", "BidSize", "OfferPrice", "OfferSize"})));

  function_definitions.insert(make_pair("Quote", FunctionDefinition(
    vector<string> {"Symbol", "Date", "Time"},
    vector<string> {"BidPrice", "BidSize", "OfferPrice", "OfferSize"})));
}
static void LoadExecutionPlan(Connection& conn);

static void ValidateRequest(Connection& conn) {
  for (string &function_name : conn.request.function_list) {
    const auto it = function_definitions.find(function_name);
    if (it != function_definitions.end()) {
      const vector<string> & function_arguments = it->second.argument_names;
      for (const string &required_argument : function_arguments) {
        int input_index = -1;
        for (size_t i = 0; i < conn.request.argument_list.size(); i ++) {
          if (conn.request.argument_list[i] == required_argument) {
            input_index = (int)i;
            break;
          }
        }
        if (input_index == -1) {
          throw invalid_argument("Missing argument:" + required_argument + " function:" + function_name);
        }
        vector<int> & argument_mapping = conn.request.functions_argument_mapping[function_name];
        argument_mapping.push_back(input_index);
      }
    } else {
      throw invalid_argument("Unknown function: " + function_name);
    }
  }
}

static void ParseRequest(Connection& conn) {
  JSONCPP_STRING err;
  Json::CharReaderBuilder builder;
  const unique_ptr<Json::CharReader> reader(builder.newCharReader());
  const string json_text =  conn.request_buffer.str();
  if (reader->parse(json_text.c_str(), json_text.c_str() + json_text.size(), &conn.request_json, &err)) {
    cout << conn.request_json;
    conn.request.id = conn.request_json["request_id"].asString();
    conn.request.separator = conn.request_json["separator"].asString();
    conn.request.response_format = conn.request_json["response_format"].asString();
    conn.request.input_cnt = conn.request_json["input_cnt"].asInt();
    conn.request.input_sorted = conn.request_json["input_sorted"].asBool();
    for (auto val : conn.request_json["function_list"]) {
      conn.request.function_list.push_back(val.asString());
    }
    for (auto val : conn.request_json["argument_list"]) {
      conn.request.argument_list.push_back(val.asString());
    }
    for (auto val : conn.request_json["result_list"]) {
      conn.request.result_list.push_back(val.asString());
    }
    ValidateRequest(conn);
    LoadExecutionPlan(conn);
    conn.request_parsed = true;
  }
}


void HandleInput(Connection & conn) {
  string line = conn.input_buffer.ReadLine();
  while (line.size()) {
    if (!conn.request_parsed) {
      conn.request_buffer << line;
      ParseRequest(conn);
    } else {
      Record record(++ conn.input_record_cnt);
      boost::split(record.values, line, boost::is_any_of(conn.request.separator));
      for (auto & plan : conn.exec_plans) {
        plan->Input(record);
      }
      if (conn.input_record_cnt == conn.request.input_cnt) {
        for (auto& plan : conn.exec_plans) {
          plan->Execute();
        }
        // ready for processing
        cout << "Done" << endl;
      }
    }
    line = conn.input_buffer.ReadLine();
  }
  conn.input_buffer.FinishReading();
}


void print_cmd(int id, const string& function, const string& symbol, Date date) {
  cout << "id:" << id << " function:" << function << " symbol:" << symbol << " date:"
    << boost::gregorian::to_iso_extended_string(date) << endl;
}
class TestLoad : public ExecutionPlan {
  // argument list: "Function", "Symbol", "Date"
public:
  TestLoad(const vector<int>& argument_mapping, bool sorted_input = true)
    : ExecutionPlan(argument_mapping, sorted_input) {}
  void Input(const Record& input_record) override {
    const string& function = input_record.values[argument_mapping[0]];
    const string& symbol = input_record.values[argument_mapping[1]];
    const Date date = MkTaqDate(input_record.values[argument_mapping[2]]);
    arguments.push_back(make_tuple(input_record.id, function, symbol, date));
  };
  void Execute() override {
    for (const auto arg : arguments) {
      apply(print_cmd, arg);
    }
  }
private:
  vector<tuple<int, string, string, Date>> arguments;
};

static void LoadExecutionPlan(Connection & conn) {
  const Request& request = conn.request;
  for (const string & function_name : request.function_list) {
    const auto it = request.functions_argument_mapping.find(function_name);
    if (it == request.functions_argument_mapping.end()) {
      throw invalid_argument("Not implemented function:" + function_name);
    }
    else if (function_name == "TestLoad") {
      conn.exec_plans.push_back(make_unique<TestLoad>(it->second, request.input_sorted));
    }
  }
}

}
