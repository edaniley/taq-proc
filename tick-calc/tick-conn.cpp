
#include "tick-calc.h"
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

static const auto nulltime = Timestamp();

void Connection::LoadExecutionPlans() {
  map<string, pair<int, int>> function_call_counts;;
  for (const auto & function_args : request.input_arguments) {
    auto it = function_call_counts.emplace(function_args.function_name, make_pair(1, 0));
    if (it.second == false) {
      ++it.first->second.first;
    }
  }
  for (const auto & function_args : request.input_arguments) {
    const string& function_name = function_args.function_name;
    string plan_name(function_args.alias);
    if (plan_name.empty()) {
      ostringstream ss;
      ss << function_name;
      pair<int, int>& counts = function_call_counts[function_name];
      if (counts.first > 1) {
        ss << '_' <<  ++ counts.second;
      }
      plan_name = ss.str();
    }
    const ArgList & arguments = function_args.arguments;
    const FunctionDef& function_def = FindFunctionDefinition(function_name);
    shared_ptr<ExecutionPlan> plan;
    if (function_name == "NBBOPrice") {
      plan = make_shared<NBBO::ExecutionPlan>(plan_name, function_def, request, arguments, NBBO::Type::PriceOnly);
    } else if (function_name == "NBBO") {
      plan = make_shared<NBBO::ExecutionPlan>(plan_name, function_def, request, arguments, NBBO::Type::PriceAndQty);
    } else if (function_name == "VWAP") {
      plan = make_shared<VWAP::ExecutionPlan>(plan_name, function_def, request, arguments);
    } else if (function_name == "ROD") {
      plan = make_shared<ROD::ExecutionPlan>(plan_name, function_def, request, arguments);
    }
    function_def.ValidateArgumentList(plan->argument_mapping);
    exec_plans.push_back(plan);
    todo_list.push_back(plan);
  }
}

void Connection::ParseRequest() {
  bool parsed = false;
  try {
    request_json = StringToJson(request_buffer.str());
    parsed = true;
  }
  catch (...) {
  }
  if (parsed) {
    request.id = request_json.get<string>("request_id", "");
    request.input_cnt = request_json.get<int>("input_cnt", 0);
    request.input_sorted = request_json.get<bool>("input_sorted", false);
    request.tz_name = request_json.get<string>("time_zone", "UTC");
    if (request.tz_name == "UTC") {
    }
    else if (request.tz_name == "America/New_York" || request.tz_name == "US/Eastern") {
    }
    else {
      throw invalid_argument("Unknown or unsupported time-zone:" + request.tz_name);
    }
    request.input_arguments = ReadArgumentList(request_json.get_child("argument_mapping"));
    LoadExecutionPlans();
    request_parsed = true;
  }
}

void Connection::PushInput() {
  string line = input_buffer.ReadLine();
  while (line.size()) {
    if (!request_parsed) {
      // accumulate lines in a buffer and attempt to parse json
      // after parsing succeeds the request is validated and execution plan is loaded
      request_buffer << line;
      ParseRequest();
    }
    else { // all subsequent lines represent input records
      InputRecord record(++ input_record_cnt);
      Split(record.values, line, '|');
      for (auto& plan : todo_list) {
        plan->Input(record);
      }
      if (input_record_cnt == request.input_cnt) {
        for (auto& plan : todo_list) {
          plan->StartExecution();
        }
        execution_started = boost::posix_time::microsec_clock::local_time();
      }
    }
    line = input_buffer.ReadLine();
  }
  input_buffer.FinishReading();
}

string Connection::BuildHeaderJson() {
  Json root, result_fields, error_summary;
  map<ErrorType, int> errors;
  for (auto &plan : exec_plans) {
    // result_fields
    Json ret_flds;
    for (const auto &fld_def : plan->output_fields) {
      Json fld, name, type;
      name.put("", fld_def.name); fld.push_back(make_pair("", name));
      type.put("", fld_def.type); fld.push_back(make_pair("", type));
      ret_flds.push_back(make_pair("", fld));
    }
    result_fields.add_child(plan->name, ret_flds);
    // error_summary
    for (const auto err: plan->errors) {
      auto it = errors.emplace(err.first, err.second); // type, count
      if (false == it.second) {
        it.first->second += err.second;
      }
    }
  }

  for (const auto& err : errors) {
    Json entry;
    entry.put("type", ErrorToString(err.first));
    entry.put("count", err.second);
    error_summary.push_back(make_pair("", entry));
  }

  root.put("request_id", request.id);
  root.put("output_records", output_record_count);
  root.add_child("result_fields", result_fields);
  root.add_child("error_summary", error_summary);
  return JsonToString(root);
}

string Connection::BuildTrailerJson() {
  Json runtime_summary;
  Timestamp exec_start, exec_end;
  for (auto& plan : exec_plans) {
    exec_start = exec_start == nulltime ? plan->execution_started : min(exec_start, plan->execution_started);
    exec_end = exec_end == nulltime ? plan->execution_ended : max(exec_end, plan->execution_ended);
  }
  runtime_summary.put("request_parsing_sorting", boost::posix_time::to_simple_string(exec_start - created));
  runtime_summary.put("execution", boost::posix_time::to_simple_string(exec_end - exec_start));
  runtime_summary.put("result_merging_sorting",
    boost::posix_time::to_simple_string(boost::posix_time::microsec_clock::local_time() - execution_ended));
  return JsonToString(runtime_summary);
}

bool Connection::CheckOutputReady() {
  if (execution_started != nulltime) {
    // move every completed plan from todo_list to done_list
    for (auto it = todo_list.begin(); it != todo_list.end(); ) {
      if ((*it)->CheckState() == ExecutionPlan::State::Done) {
        it = todo_list.erase(it);
      } else {
        ++it;
      }
    }
  }
  const bool ready = execution_started != nulltime && todo_list.empty();
  if (ready && execution_ended == nulltime) {
    execution_ended = boost::posix_time::microsec_clock::local_time();
    // merge sub-results' IDs into unique super-set of IDs
    vector<int> ids, tmp;
    for (auto& plan : exec_plans) {
      set_union(ids.begin(), ids.end(), plan->output_record_ids.begin(), plan->output_record_ids.end(), back_inserter(tmp));
      ids = move(tmp);
    }
    output_record_count = (int)ids.size();

    // prepare for merging records
    output_record_builder.Prepare(exec_plans);

    // write out 1st line - execution summary json
    string json_str = BuildHeaderJson();
    output_buffer.Write(json_str.c_str(), (int)json_str.size());
  }
  return ready;
}

void Connection::PullOutput() {
  if (false == CheckOutputReady() || exit_ready)
    return;

  while (output_records_sent < output_record_count) {
    const string output_record = output_record_builder.Build();
    if (output_buffer.AvailableSize() < (int)output_record.size())
      break;
    output_buffer.Write(output_record.c_str(), (int)output_record.size());
    output_records_sent ++;
    output_record_builder.Advance();
  }

  if (output_records_sent == output_record_count) {
    const string json_str = BuildTrailerJson();
    if ((int)json_str.size() <= output_buffer.AvailableSize()) {
      output_buffer.Write(json_str.c_str(), (int)json_str.size());
      exit_ready = true;
    }
  }
}

}
