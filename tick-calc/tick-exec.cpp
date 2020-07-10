
#ifdef _MSC_VER
#pragma comment(lib, "jsoncpp.lib")
#endif
#include <iostream>
#include <exception>
#include <chrono>
#include <cstdlib>
#include "boost-algorithm-string.h"
#include "tick-calc.h"
#include "tick-conn.h"
#include "tick-data.h"
#include "tick-exec.h"
#include "tick-func.h"

using namespace std;
using namespace Taq;

namespace tick_calc {


static map<string, FunctionDefinition> function_definitions;

void InitializeFunctionDefinitions() {
  function_definitions.insert(make_pair("Quote", FunctionDefinition("Quote",
    vector<string> {"Symbol", "Timestamp"},
    vector<string> {"ID", "Timestamp", "BestBidPx", "BestBidQty", "BestOfferPx", "BestOfferQty"}
  )));

  function_definitions.insert(make_pair("ROD", FunctionDefinition("ROD",
    vector<string> {"ID", "Symbol", "Date", "StartTime", "EndTime", "Side", "OrdQty", "LimitPx", "MPA", "ExecTime", "ExecQty"},
    vector<string> {"ID", "MinusThree", "MinusTwo", "MinusOne", "Zero", "PlusOne", "PlusTwo", "PlusThree"}
  )));
}

static void LoadExecutionPlan(Connection& conn) {
  const Request& request = conn.request;
  for (const string& function_name : request.function_list) {
    const auto &function = function_definitions.find(function_name)->second;
    const auto it = request.functions_argument_mapping.find(function_name);
    if (it == request.functions_argument_mapping.end()) {
      throw invalid_argument("Not implemented function:" + function_name);
    }
    else if (function_name == "Quote") {
      conn.exec_plans.push_back(make_unique<QuoteExecutionPlan>(function, request, it->second));
    }
    else if (function_name == "ROD") {
      conn.exec_plans.push_back(make_unique<RodExecutionPlan>(function, request, it->second));
    }
  }
}

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
    cout << conn.request_json << endl;
    conn.request.id = conn.request_json["request_id"].asString();
    conn.request.separator = conn.request_json["separator"].asString();
    conn.request.response_format = conn.request_json["output_format"].asString();
    conn.request.input_cnt = conn.request_json["input_cnt"].asInt();
    conn.request.input_sorted = conn.request_json["input_sorted"].asBool();
    const string time_zone = conn.request_json.isMember("time_zone")
      ? conn.request_json["time_zone"].asString() : string("UTC");
    if (time_zone == "UTC") {
    } else if (time_zone == "America/New_York" || time_zone == "US/Eastern") {
    } else {
      throw invalid_argument("Unknown or unsupported time-zone:" + time_zone);
    }
    conn.request.tz_name = time_zone;
    for (auto val : conn.request_json["function_list"]) {
      conn.request.function_list.push_back(val.asString());
    }
    for (auto val : conn.request_json["argument_list"]) {
      conn.request.argument_list.push_back(val.asString());
    }
    ValidateRequest(conn);
    LoadExecutionPlan(conn);
    conn.request_parsed = true;
  }
}

void ConnectionPushInput(Connection & conn) {
  string line = conn.input_buffer.ReadLine();
  while (line.size()) {
    if (!conn.request_parsed) {
      // accumulate lines in a buffer and attempt to parse json
      // after parsing succeeds the request is validated and execution plan is loaded
      conn.request_buffer << line;
      ParseRequest(conn);
    } else { // all subsequent lines represent input records
      InputRecord record(++ conn.input_record_cnt);
      boost::split(record.values, line, boost::is_any_of(conn.request.separator));
      for (auto & plan : conn.exec_plans) {
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

void ConnectionPullOutput(Connection &conn) {
  size_t count_done = 0;
  for (auto & exec_plan : conn.exec_plans) {
    const ExecutionPlan::State state = exec_plan->CheckState();
    if (state == ExecutionPlan::State::Done) {
      count_done ++;
      continue;
    }
    if (state == ExecutionPlan::State::OuputReady) {
      if (const int available_size = conn.output_buffer.AvailableSize() ) {
        const int size_read = exec_plan->PullOutput(conn.output_buffer.WritePtr(), available_size);
        conn.output_buffer.CommitWrite(size_read);
      }
    }
    break;
  }
  conn.exit_ready = count_done > 0 && count_done == conn.exec_plans.size();
}

/* ===================================================== page ========================================================*/

static ProducerConsumerQueue<ExecutionUnit> exec_queue;
static vector<thread> thread_pool;

string CurrentTimestamp() {
  auto now = chrono::system_clock::now();
  auto in_time_t = chrono::system_clock::to_time_t(now);
  ostringstream ss;
  tm out_tm;
  #ifdef _MSC_VER
  localtime_s(&out_tm, &in_time_t);
  #else
  out_tm = *localtime(&in_time_t);
  #endif
  ss << put_time(&out_tm, "%Y-%m-%d %X");
  return ss.str();
}


vector<int> AvailableCpuCores(string& cpu_list) {
  vector<int> cores;
  vector<string> tokens;
  boost::split(tokens, cpu_list, boost::is_any_of(","));
  for (const string& token : tokens) {
    if (!token.empty()) {
      vector<string> range;
      boost::split(range, token, boost::is_any_of("-"));
      if (range.size() == 1) {
        cores.push_back(stoi(token.c_str()));
      }
      else if (range.size() == 2) {
        for (int core = stoi(range[0].c_str()); core <= stoi(range[1].c_str()); core++) {
          cores.push_back(core);
        }
      }
}
  }
  int num_cores = thread::hardware_concurrency();
  auto it = remove_if(cores.begin(), cores.end(), [num_cores](int core) {return core >= num_cores; });
  cores.resize(cores.size() - distance(it, cores.end()));
  sort(cores.begin(), cores.end(), less<int>());
  return vector<int>(cores.begin(), unique(cores.begin(), cores.end()));
}

bool SetThreadCpuAffinity(int cpu_core) {
#ifdef _MSC_VER
  bool success = SetThreadAffinityMask(GetCurrentThread(), (DWORD_PTR)1 << cpu_core) != 0;
#else
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_core, &cpuset);
  bool success = sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0;
#endif
  if (success) {
    cout << "Pinned current thread:" << this_thread::get_id() << " cpu-core:" << cpu_core << endl;
  }
  return success;
}

void ExecutionThread(int cpu_core) {
  cout << CurrentTimestamp() << " thread:" << this_thread::get_id() << " started" << endl;
  if (cpu_core != 1) {
    SetThreadCpuAffinity(cpu_core);
  }
  while (true) {
    shared_ptr<ExecutionUnit> job = exec_queue.Dequeue();
    if (!job.get()) {
      break;
    }
    job->Execute();
    job->done.store(true);
  }
}

void CreateThreads(const vector<int> & cpu_cores) {
  if (cpu_cores.size() > 1) {
    for (size_t i = 1; i < cpu_cores.size(); i ++) {
      thread_pool.push_back(thread(ExecutionThread, cpu_cores[i]));
    }
  } else {
    thread_pool.push_back(thread(ExecutionThread, -1));
  }
}

void DestroyThreads() {
  for (size_t i = 0; i < thread_pool.size(); i++) {
    shared_ptr<ExecutionUnit> exit_job;
    exec_queue.Enqueue(exit_job);
  }
  for (size_t i = 0; i < thread_pool.size(); i++) {
    thread_pool[i].join();
  }
}

void AddExecutionUnit(shared_ptr<ExecutionUnit> & job) {
  exec_queue.Enqueue(job);
}

/* ===================================================== page ========================================================*/

ExecutionPlan::State ExecutionPlan::CheckState() {
  // move every completed unit from todo_list to done_list
  for (auto it = todo_list.begin(); it != todo_list.end(); ) {
    if ((*it)->done.load()) {
      done_list.push_back(*it);
      it = todo_list.erase(it);
    }
    else {
      ++it;
    }
  }
  if (todo_list.empty() && output_records.empty()) {
    execution_ended = boost::posix_time::microsec_clock::local_time();
    // once : consolidate all output records into output_records, then sort by id
    size_t total_record_count = 0;
    for (const auto& exec_unit : done_list) {
      total_record_count += exec_unit->output_records.size();
      for (const auto& err : exec_unit->errors) {
        Error(err.first, err.second);
      }
    }

    output_records.reserve(total_record_count);
    for (auto& exec_unit : done_list) {
      output_records.insert(output_records.end(),
        make_move_iterator(exec_unit->output_records.begin()),
        make_move_iterator(exec_unit->output_records.end()));
      exec_unit->output_records.clear();
    }
    sort(output_records.begin(), output_records.end(), [](const auto& left, const auto& right) {
      return left.id < right.id;
      });
  }
  const bool preparing_execution = 0 == (todo_list.size() + done_list.size());
  const bool busy = todo_list.size() > 0 || preparing_execution;
  const bool execution_started = output_records.size() > 0 || errors.size() > 0;
  const bool output_available  = output_records_done < output_records.size();
  const bool ready = !busy && execution_started && output_available;
  ExecutionPlan::State state = busy ? ExecutionPlan::State::Busy
                            : ready ? ExecutionPlan::State::OuputReady : ExecutionPlan::State::Done;
  return state;
}

int ExecutionPlan::PullOutput(char* buffer, int available_size) {
  int bytes_written = 0;
  auto WriteOutput = [&](const char* str, int size) {
    memcpy(buffer, str, size);
    buffer += size;
    bytes_written += size;
    available_size -= size;
  };
  if (buffer) {
    if (false == output_header_done) {
      const string replay_header = MakeReplyHeader();
      WriteOutput(replay_header.c_str(), (int)replay_header.size());
      output_header_done = true;
    }
    while (output_records_done < output_records.size()) {
      const OutputRecord& rec = output_records[output_records_done];
      const int rec_size = (int)rec.value.size();
      if (available_size < rec_size) {
        break;
      }
      WriteOutput(rec.value.c_str(), rec_size);
      output_records_done++;
    }
  }
  return bytes_written;
}

void ExecutionPlan::Error(ErrorType error_type, int count) {
  auto ret = errors.insert(make_pair(error_type, 0));
  if (ret.second) {
    ret.first->second = count;
  } else {
    ret.first->second += count;
  }
}

string ExecutionPlan::MakeReplyHeader() const {
  Json::Value runtime_summary;
  runtime_summary["parsing_input"] = boost::posix_time::to_simple_string(execution_started - created);
  runtime_summary["execution"] = boost::posix_time::to_simple_string(execution_ended - execution_started);
  runtime_summary["sorting_output"] = boost::posix_time::to_simple_string(boost::posix_time::microsec_clock::local_time() - execution_ended);

  Json::Value output_fields;
  for (const auto & field : function.output_fields) {
    output_fields.append(field);
  }
  Json::Value error_summary;
  for (const auto& error_cnt : errors) {
    Json::Value error;
    error["type"] = ErrorToString(error_cnt.first);
    error["count"] = error_cnt.second;
    error_summary.append(error);
  }
  Json::Value root;
  root["request_id"] = request.id;
  root["output_fields"] = output_fields;
  root["output_records"] = output_records.size();
  root["error_summary"] = error_summary;
  root["runtime_summary"] = runtime_summary;

  Json::StreamWriterBuilder builder;
  std::string output = Json::writeString(builder, root);
  cout << output << endl;
  output.erase(remove_if(output.begin(), output.end(), [](char c) {return c == '\n';}), output.end());
  replace(output.begin(), output.end(), '\t', ' ');
  output.append("\n");
  return output;
}

}