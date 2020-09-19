
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
#include "tick-json.h"

using namespace std;
using namespace Taq;

namespace tick_calc {


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
    ostringstream ss;
    ss << "Pinned current thread:" << this_thread::get_id() << " cpu-core:" << cpu_core;
    Log(LogLevel::INFO, ss.str());
  }
  return success;
}

void ExecutionThread(int cpu_core) {
  ostringstream ss;
  ss << CurrentTimestamp() << " thread:" << this_thread::get_id() << " started";
  Log(LogLevel::INFO, ss.str());
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
  static const auto nulltime = boost::posix_time::ptime();
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
  if (todo_list.empty() && execution_started != nulltime && execution_ended == nulltime) {
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

  const bool done = execution_ended != nulltime;
  const bool output_available = done && (output_records_done < output_records.size() || false  == output_header_done);
  ExecutionPlan::State state = false == done ? ExecutionPlan::State::Busy
                            : output_available ? ExecutionPlan::State::OuputReady : ExecutionPlan::State::Done;
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
  js::ptree runtime_summary;
  runtime_summary.put("parsing_input",  boost::posix_time::to_simple_string(execution_started - created));
  runtime_summary.put("execution", boost::posix_time::to_simple_string(execution_ended - execution_started));
  runtime_summary.put("sorting_output", boost::posix_time::to_simple_string(boost::posix_time::microsec_clock::local_time() - execution_ended));

  js::ptree output_fields;
  const vector<string>& result_fields = ResultFields();
  for (const auto & field : result_fields) {
    js::ptree fld;
    fld.put("", field);
    output_fields.push_back(make_pair("", fld));
  }
  js::ptree error_summary;
  for (const auto& error_cnt : errors) {
    js::ptree err;
    err.put("type", ErrorToString(error_cnt.first));
    err.put("count", error_cnt.second);
    error_summary.push_back(make_pair("", err));
  }
  js::ptree root;
  root.put("request_id", request.id);
  root.add_child("output_fields", output_fields);
  root.put("output_records", output_records.size());
  root.add_child("error_summary", error_summary);
  root.add_child("runtime_summary", runtime_summary);

  return JsonToString(root);
}

}
