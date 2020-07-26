#include <queue>
#include <mutex>
#include <iostream>
#include <memory>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>

#include "tick-calc.h"

using namespace std;
using namespace Taq;

namespace tick_calc {


struct LogMsg {
  LogMsg(LogLevel level, const string &message)
    : time(boost::posix_time::microsec_clock::local_time()), level(level), message(message) {}
  ~LogMsg() {cout << "-- LogMsg" << endl;}
  const Timestamp time;
  const LogLevel level;
  const string message;
  static const char *LevelToString(LogLevel level) {
    switch (level) {
    case LogLevel::INFO: return "INFO";
    case LogLevel::ERR: return "ERROR";
    case LogLevel::WARN: return "WARNING";
    case LogLevel::CRITICAL: return "CRITICAL";
    case LogLevel::DEBUG: return "DEBUG";
    }
    return "";
  }
};

#ifdef __unix__
static auto pid = getpid();
#else
static auto pid = GetCurrentProcessId();
#endif
static queue<unique_ptr<LogMsg>> message_queue;
static mutex log_lock;
static std::ofstream log;

void LogInitialize(AppAruments &args) {
  ostringstream ss;
  boost::filesystem::path file_path(args.log_dir);
  boost::gregorian::date today = boost::posix_time::microsec_clock::local_time().date();
  ss << boost::gregorian::to_iso_string(today) << ".tick-calc." << pid << ".log";
  file_path /= ss.str();
  const string log_file = file_path.string();
  log.open(log_file, ios::out | ios::app);
}

void LogWrite(const LogMsg& log_msg) {
  log << boost::posix_time::to_iso_extended_string(log_msg.time)
    << '|' << LogMsg::LevelToString(log_msg.level)
    << '|' << log_msg.message << endl;
}

void Log(LogLevel log_level, const string& message) {
  unique_lock<mutex> lock(log_lock);
  message_queue.emplace(make_unique<LogMsg>(log_level, message));
}

void LogPoll() {
  unique_lock<mutex> lock(log_lock);
  if (false == message_queue.empty()) {
    auto msg = move(message_queue.front());
    message_queue.pop();
    LogWrite(*msg);
  }
}

void LogFinalize(AppAruments&) {
  unique_lock<mutex> lock(log_lock);
  while (false == message_queue.empty()) {
    auto msg = move(message_queue.front());
    message_queue.pop();
    LogWrite(*msg);
  }
  log.flush();
  log.close();
}

}


