#ifndef TICK_EXEC_INCLUDED
#define TICK_EXEC_INCLUDED

#include <string>
#include <vector>
#include <list>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "taq-proc.h"
#include "tick-data.h"
#include "tick-request.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

class FunctionDefinition {
public:
  using Validator = function<void(const vector<int> & argument_mapping)>;
  FunctionDefinition(const string &name, const vector<string> & argument_names, const vector<string>& output_fields, Validator validator = Validator())
    : name(name), argument_names(argument_names), output_fields(output_fields) , validator(validator) { }
  const string name;
  const vector<string> argument_names;
  const vector<string> output_fields;
  Validator validator;
  void ValidateArgumentList(const vector<int>& argument_mapping) const {
    if (validator) {  // use custom validator
      validator(argument_mapping);
    } else {          // use default validator expects all arguments present
      for (size_t i = 0; i < argument_names.size(); i++) {
        if (argument_mapping[i] == -1) {
          throw invalid_argument("Missing argument:" + argument_names[i] + " function:" + name);
        }
      }
    }
  }
};

template <typename T>
class ProducerConsumerQueue {
public:
  void Enqueue(shared_ptr<T> obj) {
    unique_lock<mutex> lock(mtx_);
    que_.push_back(obj);
    cv_.notify_one();
  }
  shared_ptr<T> Dequeue() {
    unique_lock<mutex> lock(mtx_);
    while (que_.size() == 0) {
      cv_.wait(lock);
    }
    shared_ptr<T> obj = que_.front();
    que_.pop_front();
    return obj;
  }
  size_t Purge() {
    unique_lock<mutex> lock(mtx_);
    const size_t purged = Size();
    que_.clear();
    return purged;
  }
  size_t Size() const { return que_.size(); }
  bool Empty() const { return Size() == 0; }
private:
  mutex mtx_;
  condition_variable cv_;
  list<shared_ptr<T>> que_;
};

class ExecutionUnit {
public:
  ExecutionUnit() { done.store(false); }
  virtual ~ExecutionUnit() {};
  virtual void Execute() = 0;
  void Error(ErrorType error_type, int count = 1) {
    auto ret = errors.insert(make_pair(error_type, 0));
    if (ret.second) {
      ret.first->second = count;
    } else {
      ret.first->second += count;
    }
  }
  atomic<bool> done;
  OutputRecordset output_records;
  map<ErrorType, int> errors;
};

class ExecutionPlan {
public:
  enum class State {Busy, OuputReady, Done};
  ExecutionPlan(const FunctionDefinition & function, const Request & request, const vector<int>& argument_mapping)
      : function(function), request(request), argument_mapping(argument_mapping),
        output_records_done(0), output_header_done(false), record_cnt(0) {
        created = boost::posix_time::microsec_clock::local_time();
      }
  virtual ~ExecutionPlan() {};
  virtual void Input(InputRecord&) = 0;
  virtual void Execute() = 0;
  void StartExecution() {
    execution_started = boost::posix_time::microsec_clock::local_time();
    Execute();
  }
  State CheckState();
  int PullOutput(char * buffer, int available_size);
protected:
  void Error(ErrorType error_type, int count = 1);
  string MakeReplyHeader() const;
  const FunctionDefinition & function;
  const Request & request;
  const vector<int> argument_mapping;
  vector<shared_ptr<ExecutionUnit>> todo_list;
  vector<shared_ptr<ExecutionUnit>> done_list;
  OutputRecordset output_records;
  size_t output_records_done;
  bool output_header_done;
  size_t record_cnt;// remove
  map<ErrorType, int> errors;
  boost::posix_time::ptime created;
  boost::posix_time::ptime execution_started;
  boost::posix_time::ptime execution_ended;
};

// public routines
void InitializeFunctionDefinitions();
vector<int> AvailableCpuCores(string& cpu_list);
bool SetThreadCpuAffinity(int cpu_core);
void ExecutionThread(int core);
void CreateThreads(const vector<int> &cpu_cores);
void DestroyThreads();
void AddExecutionUnit(shared_ptr<ExecutionUnit> &);

}

#endif
