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

struct Connection {
  Connection() : fd(-1), request_buffer(), request_parsed(false), input_record_cnt(0),
                 output_ready(false), exit_ready(false) {}
  Connection(int fd) : Connection() {this->fd = fd;}
  int fd;
  Request       request;
  stringstream  request_buffer;
  js::ptree     request_json;
  bool          request_parsed;
  LineBuffer<1024 * 50> input_buffer;
  int           input_record_cnt;
  vector<unique_ptr<ExecutionPlan>> exec_plans;
  OutputBuffer< 1024 * 10>  output_buffer;
  bool          output_ready;
  bool          exit_ready;
};

void ConnectionPushInput(Connection& conn);
void ConnectionPullOutput(Connection& conn);

}

#endif

