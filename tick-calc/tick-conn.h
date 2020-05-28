#ifndef TICK_CONN_INCLUDED
#define TICK_CONN_INCLUDED

#include <string_view>
#include <vector>
#include <algorithm>
#include <functional>
#include <sstream>

#include <json/json.h>

#include "taq-proc.h"

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
      throw overflow_error("tick_calc::LineBuffer overflow");
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

struct Connection {
  Connection() : fd(-1), sep('|'), input_buffer(), request_buffer(), request_parsed(false), writable(false) {}
  Connection(int fd) : fd(fd), sep('|'), input_buffer(), request_buffer(), request_parsed(false), writable(false){}
  int fd;
  char sep;
  LineBuffer<1024*10> input_buffer;
  stringstream request_buffer;
  Json::Value request;
  bool request_parsed;
  bool writable;
};

bool HandleInput(Connection& conn);
}

#endif

