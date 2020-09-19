
#include "tick-conn.h"
#include "tick-exec.h"
#include "taq-text.h"
#include "tick-json.h"
#include "tick-func-vwap.h"
#include "tick-func-nbbo.h"
#include "tick-func-rod.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

static void LoadExecutionPlan(Connection& conn) {
  const Request& request = conn.request;
  for (const string& function_name : request.function_list) {
    const FunctionDefinition& function = FindFunctionDefinition(function_name);
    const auto it = request.functions_argument_mapping.find(function_name);
    if (it == request.functions_argument_mapping.end()) {
      throw invalid_argument("Not implemented function:" + function_name);
    }
    else if (function_name == "NBBOPrice") {
      conn.exec_plans.push_back(make_unique<NBBO::ExecutionPlan>(function, request, it->second, NBBO::Type::PriceOnly));
    }
    else if (function_name == "NBBO") {
      conn.exec_plans.push_back(make_unique<NBBO::ExecutionPlan>(function, request, it->second, NBBO::Type::PriceAndQty));
    }
    else if (function_name == "VWAP") {
      conn.exec_plans.push_back(make_unique<VWAP::ExecutionPlan>(function, request, it->second));
    }
    else if (function_name == "ROD") {
      conn.exec_plans.push_back(make_unique<ROD::ExecutionPlan>(function, request, it->second));
    }
  }
}

static void ValidateRequest(Connection& conn) {
  for (string& function_name : conn.request.function_list) {
    const FunctionDefinition& function_def = FindFunctionDefinition(function_name);
    vector<int> argument_mapping = function_def.MapArgumentList(conn.request.argument_list);
    function_def.ValidateArgumentList(argument_mapping);
    conn.request.functions_argument_mapping.insert(make_pair(function_name, move(argument_mapping)));
  }
}

static void ParseRequest(Connection& conn) {
  bool parsed = false;
  try {
    conn.request_json = StringToJson(conn.request_buffer.str());
    parsed = true;
  }
  catch (...) {
  }
  if (parsed) {
    conn.request.id = conn.request_json.get<string>("request_id", "");
    conn.request.separator = conn.request_json.get<string>("separator", "|");
    conn.request.output_format = conn.request_json.get<string>("output_format", "psv");
    conn.request.input_cnt = conn.request_json.get<int>("input_cnt", 0);
    conn.request.input_sorted = conn.request_json.get<bool>("input_sorted", false);
    conn.request.tz_name = conn.request_json.get<string>("time_zone", "UTC");
    if (conn.request.tz_name == "UTC") {
    }
    else if (conn.request.tz_name == "America/New_York" || conn.request.tz_name == "US/Eastern") {
    }
    else {
      throw invalid_argument("Unknown or unsupported time-zone:" + conn.request.tz_name);
    }
    conn.request.function_list = AsVector<string>(conn.request_json, "function_list");
    conn.request.argument_list = AsVector<string>(conn.request_json, "argument_list");
    ValidateRequest(conn);
    LoadExecutionPlan(conn);
    conn.request_parsed = true;
  }
}

void ConnectionPushInput(Connection& conn) {
  string line = conn.input_buffer.ReadLine();
  while (line.size()) {
    if (!conn.request_parsed) {
      // accumulate lines in a buffer and attempt to parse json
      // after parsing succeeds the request is validated and execution plan is loaded
      conn.request_buffer << line;
      ParseRequest(conn);
    }
    else { // all subsequent lines represent input records
      InputRecord record(++conn.input_record_cnt);
      Split(record.values, line, conn.request.separator[0]);
      for (auto& plan : conn.exec_plans) {
        plan->Input(record);
      }
      if (conn.input_record_cnt == conn.request.input_cnt) {
        for (auto& plan : conn.exec_plans) {
          plan->StartExecution();
        }
      }
    }
    line = conn.input_buffer.ReadLine();
  }
  conn.input_buffer.FinishReading();
}

void ConnectionPullOutput(Connection& conn) {
  size_t count_done = 0;
  for (auto& exec_plan : conn.exec_plans) {
    const ExecutionPlan::State state = exec_plan->CheckState();
    if (state == ExecutionPlan::State::Done) {
      count_done++;
      continue;
    }
    if (state == ExecutionPlan::State::OuputReady) {
      if (const int available_size = conn.output_buffer.AvailableSize()) {
        const int size_read = exec_plan->PullOutput(conn.output_buffer.WritePtr(), available_size);
        conn.output_buffer.CommitWrite(size_read);
      }
    }
    break;
  }
  conn.exit_ready = count_done > 0 && count_done == conn.exec_plans.size();
}

}
