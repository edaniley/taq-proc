#ifndef TICK_CONN_INCLUDED
#define TICK_CONN_INCLUDED

#include <vector>
#include <algorithm>
#include <functional>
#include <sstream>
#include <exception>
#include "taq-proc.h"
#include "tick-exec.h"
#include "tick-json.h"
#include "tick-output-builder.h"

using namespace std;
using namespace Taq;


namespace tick_calc {

template<int SIZE>
class LineBuffer {
public:
  LineBuffer() : read_ptr_(data_), write_ptr_(data_) {}
  size_t AvailableSize() const {
    int retval = max(0, SIZE - (int)(write_ptr_ - data_));
    return (size_t)retval;
  }
  void Write(const char* data, size_t length) {
    if (AvailableSize() < length) {
      throw overflow_error("Input buffer overflow");
    }
    memcpy(write_ptr_, data, length);
    write_ptr_ += length;
  }
  char * BeginWrite() {
    return write_ptr_;
  }
  void CommitWrite(size_t length) {
    write_ptr_ += length;
  }
  string ReadLine() {
    if (char* nl = (char*)memchr(read_ptr_, '\n', write_ptr_ - read_ptr_)) {
      *nl = 0;
      string retval(read_ptr_);
      read_ptr_ = nl + 1;
      return retval;
    }
    return "";
  }
  void FinishReading() {
    const size_t read_size = write_ptr_ - read_ptr_;
    memmove(data_, read_ptr_, read_size);
    read_ptr_ = data_;
    write_ptr_ = data_ + read_size;
    memset(write_ptr_, '.', AvailableSize());
  }
private:
  char data_[SIZE];
  char* read_ptr_;
  char* write_ptr_;
};

template <int SIZE>
class OutputBuffer {
public:
  OutputBuffer() { Reset(); }
  char* Data() const { return read_ptr_; }
  int DataSize() const { return (int)(write_ptr_ - read_ptr_); }
  int AvailableSize() const { return SIZE - DataSize(); }
  int Write(const char* data, int datalen) {
    int write_size = std::min(AvailableSize(), datalen);
    memcpy(write_ptr_, data, write_size);
    write_ptr_ += write_size;
    return write_size;
  }
  char* WritePtr() { return write_ptr_; }
  void CommitWrite(int write_size) { write_ptr_ += write_size; }
  void CommitRead(int read_size) { read_ptr_ += read_size; }
  void Reset() { read_ptr_ = write_ptr_ = data_; }
private:
  char data_[SIZE];
  char* read_ptr_;
  char* write_ptr_;
};

class Connection {
public:
  Connection(int fd, const string &ip, uint16_t tcp)
    : fd(fd), ip(ip), tcp(tcp),
      output_ready(false), exit_ready(false), request_parsed(false),
      input_record_cnt(0), output_record_count(0), output_records_sent(0) {
        created = boost::posix_time::microsec_clock::local_time();
      }
  void PushInput();
  void PullOutput();
private:
  void ParseRequest();
  void LoadExecutionPlans();
  bool CheckOutputReady();
  string BuildHeaderJson();
  string BuildTrailerJson();
public:
  int             fd;
  const string    ip;
  const uint16_t  tcp;
  LineBuffer<1024 * 50> input_buffer;
  OutputBuffer< 1024 * 50>  output_buffer;
  bool          output_ready; // for epoll on Linux
  bool          exit_ready;
private:
  Request       request;
  stringstream  request_buffer;
  Json          request_json;
  bool          request_parsed;
  int           input_record_cnt;
  int           output_record_count;
  int           output_records_sent;
  Timestamp     created;
  Timestamp     execution_started;
  Timestamp     execution_ended;
  vector<shared_ptr<ExecutionPlan>> exec_plans;
  vector<shared_ptr<ExecutionPlan>> todo_list;
  OutputRecordBuilder output_record_builder;
};

}

#endif

