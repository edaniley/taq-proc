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

using namespace std;
using namespace Taq;

namespace tick_calc {

struct FunctionDefinition {
  FunctionDefinition(const vector<string> & argument_names) : argument_names(argument_names) {}
  const vector<string> argument_names;
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
  ExecutionUnit() { done.store(false); cout << "+ ExecutionUnit" << endl;}
  virtual ~ExecutionUnit() { cout << "- ExecutionUnit" << endl; };
  virtual void Execute() = 0;
  atomic<bool> done;
  OutputRecordset output_records;
};

class ExecutionPlan {
public:
  enum class State {Busy, OuputReady, Done};
  ExecutionPlan(const vector<int>& argument_mapping, const string &field_separator, bool sorted_input = true) :
    argument_mapping(argument_mapping), field_separator(field_separator),
    sorted_input(sorted_input), output_records_done(0) { cout << "+ ExecutionPlan" << endl; }
  virtual ~ExecutionPlan() {cout << "- ExecutionPlan" << endl;};
  virtual void Input(InputRecord&) = 0;
  virtual void Execute() = 0;
  virtual State CheckState() = 0;
  virtual int PullOutput(char * buffer, int available_size) = 0;
  const vector<int> argument_mapping;
  const string field_separator;
  const bool sorted_input;
  vector<shared_ptr<ExecutionUnit>> todo_list;
  vector<shared_ptr<ExecutionUnit>> done_list;
  OutputRecordset output_records;
  size_t output_records_done;
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
