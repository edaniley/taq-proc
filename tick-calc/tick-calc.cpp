
#include <iostream>
#include <cassert>
#include <thread>
#include <sstream>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <csignal>
#include "taq-proc.h"
#include "tick-calc.h"
#include "tick-data.h"
#include "tick-conn.h"
#include "tick-exec.h"

using namespace std;
using namespace Taq;
using namespace tick_calc;

namespace po = boost::program_options;
namespace fs = boost::filesystem;

static bool verbose = false;
bool tick_calc::IsVerbose() {
  return verbose;
}

static bool ValidateCmdArgs(AppAruments& args) {
  if (false == (fs::exists(args.in_data_dir) && fs::is_directory(args.in_data_dir)) ) {
    cerr << "Invalid --data-dir: " << args.in_data_dir << endl;
    return false;
  }
  return true;
}

atomic<bool> exit_signal(false);
#ifdef __unix__
void ExitSignalHandler(int) {
  exit_signal.store(true);
}
#else
BOOL ExitSignalHandler(DWORD) {
  exit_signal.store(true);
  return true;
}
#endif

int main(int argc, char **argv) {
  int retval = 0;
  AppAruments args;
  po::options_description desc("program options");
  desc.add_options()
    ("help,h", "produce help message")
    ("data-dir,d", po::value<string>(&args.in_data_dir)->default_value("."), "data directory")
    ("log_dir,l", po::value<string>(&args.log_dir)->default_value("."), "log directory")
    ("-tcp,t", po::value<uint16_t>(&args.in_port)->default_value(3090), "TCP port")
    ("-cpu,c", po::value<string>(&args.in_cpu_list), "CPU core list to pin threads")
    ("-verbose,v", po::value<bool>(&verbose)->default_value(false), "vebose mode with output written to stdout")
    ;
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  } if (false == ValidateCmdArgs(args)) {
    return 2;
  }
  vector<int> cpu_cores = AvailableCpuCores(args.in_cpu_list);
  if (cpu_cores.size() > 1) {
    SetThreadCpuAffinity(cpu_cores[0]);
  }
  #ifdef __unix__
    signal(SIGTERM, ExitSignalHandler);
    signal(SIGQUIT, ExitSignalHandler);
  #else
    SetConsoleCtrlHandler(ExitSignalHandler, true);
  #endif
  try {
    LogInitialize(args);
    InitializeData(args.in_data_dir);
    NetInitialize(args);
    CreateThreads(cpu_cores);
    Log(LogLevel::INFO, "Ready");
    while (!exit_signal.load()) {
      NetPoll();
      LogPoll();
    }
    Log(LogLevel::INFO, "Exit requested");
  }
  catch (exception& ex) {
    Log(LogLevel::CRITICAL, ex.what());
  }
  DestroyThreads();
  CleanupData();
  NetFinalize(args);
  Log(LogLevel::INFO, "Done");
  LogFinalize(args);
  return retval;
}
