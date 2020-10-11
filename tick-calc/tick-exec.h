#ifndef TICK_EXEC_INCLUDED
#define TICK_EXEC_INCLUDED

#include <string>
#include <vector>
#include <list>
#include <map>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "taq-proc.h"
#include "tick-data.h"
#include "tick-func.h"
#include "tick-request.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

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

struct InputRecord {
  InputRecord(int id) : id(id) {}
  const int id;
  vector<string_view> values;
};

typedef vector<InputRecord> InputRecordSet;

struct InputRecordRange {
  InputRecordRange(const InputRecordSet& records, size_t first, size_t end)
    : records(records), first(first), end(end) {}
  const InputRecordSet& records;
  size_t first;
  size_t end;
};

struct OutputRecord {
  OutputRecord(int id) : id(id) {}
  OutputRecord(int id, const string& data) : id(id), data(data) {}
  int id;
  string data;
};

typedef vector<OutputRecord> OutputRecordset;

class ExecutionUnit {
public:
  ExecutionUnit(const string &symbol, Date date, bool adjust_time)
    : symbol(symbol), date(date), taq_time_adjustment(adjust_time ? UtcToTaq(date) : ZeroTime()) {
    done.store(false);
  }
  virtual ~ExecutionUnit() {};
  virtual void Execute() = 0;
  void Error(ErrorType error_type, int count = 1) {
    auto ret = errors.insert(make_pair(error_type, count));
    if (false == ret.second) {
      ret.first->second += count;
    }
  }
  const string symbol;
  const Date date;
  const Time taq_time_adjustment;
  atomic<bool> done;
  OutputRecordset output_records;
  map<ErrorType, int> errors;
};

class Connection;
class OutputRecordBuilder;

class ExecutionPlan {
friend class Connection;
friend class OutputRecordBuilder;
public:
  virtual ~ExecutionPlan() {};
  enum class State {Busy, Done};
protected:
  ExecutionPlan(const string& name, const FunctionDef& function_def, const Request& request, const ArgList& arg_list);
private:
  virtual void Input(InputRecord&) = 0;
  virtual void Execute() = 0;

  void StartExecution() {
    execution_started = boost::posix_time::microsec_clock::local_time();
    Execute();
    SetResultFields();
  }
  State CheckState();
  void SetResultFields();
protected:
  void Error(ErrorType error_type, int count = 1);

  const string name;
  const FunctionDef& function_def;
  const Request & request;
  vector<int> argument_mapping;
  vector<shared_ptr<ExecutionUnit>> todo_list;
  vector<shared_ptr<ExecutionUnit>> done_list;
  vector<FieldDef> output_fields;
  OutputRecordset output_records;
  vector<int> output_record_ids;
  size_t markouts_size;
  map<ErrorType, int> errors;
  Timestamp created;
  Timestamp execution_started;
  Timestamp execution_ended;
};

// public routines
vector<int> AvailableCpuCores(string& cpu_list);
bool SetThreadCpuAffinity(int cpu_core);
void ExecutionThread(int core);
void CreateThreads(const vector<int> &cpu_cores);
void DestroyThreads();
void AddExecutionUnit(shared_ptr<ExecutionUnit> &);

}

#endif
