
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
//using namespace tick_calc;

namespace po = boost::program_options;
namespace fs = boost::filesystem;

static bool ValidateCmdArgs(tick_calc::AppAruments& args) {
  if (false == (fs::exists(args.in_data_dir) && fs::is_directory(args.in_data_dir)) ) {
    cerr << "Invalid --data-dir: " << args.in_data_dir << endl;
    return false;
  }
  return true;
}

atomic<bool> exit_signal(false);
void ExitSignalHandler(int) {
  exit_signal.store(true);
}

int main(int argc, char **argv) {
  int retval = 0;
  tick_calc::AppAruments args;
  po::options_description desc("program options");
  desc.add_options()
    ("help,h", "produce help message")
    ("data-dir,d", po::value<string>(&args.in_data_dir)->default_value("."), "output directory")
    ("-tcp,t", po::value<uint16_t>(&args.in_port)->default_value(21090), "TCP port")
    ("-cpu,c", po::value<string>(&args.in_cpu_list), "CPU core list to pin threads")
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
  vector<int> cpu_cores = tick_calc::AvailableCpuCores(args.in_cpu_list);
  if (cpu_cores.size() > 1) {
    tick_calc::SetThreadCpuAffinity(cpu_cores[0]);
  }

  tick_calc::InitializeData(args.in_data_dir);
  tick_calc::InitializeFunctionDefinitions();
  NetInitialize(args);
  tick_calc::CreateThreads(cpu_cores);
  signal(SIGTERM, ExitSignalHandler);
  try {
    while (!exit_signal.load()) {
        NetPoll(args);
    }
  }
  catch (exception& ex) {
    cerr << ex.what();
  }
  tick_calc::DestroyThreads();
  tick_calc::CleanupData();
  cout << " In " __FILE__ "\n";
  return retval;
}
