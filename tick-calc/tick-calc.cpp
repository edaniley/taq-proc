
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

static bool ValidateCmdArgs(tick_calc::AppContext & ctx) {
  if (false == (fs::exists(ctx.in_data_dir) && fs::is_directory(ctx.in_data_dir)) ) {
    cerr << "Invalid --data-dir: " << ctx.in_data_dir << endl;
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
  tick_calc::AppContext ctx;
  po::options_description desc("program options");
  desc.add_options()
    ("help,h", "produce help message")
    ("data-dir,d", po::value<string>(&ctx.in_data_dir)->default_value("."), "output directory")
    ("-tcp,t", po::value<uint16_t>(&ctx.in_port)->default_value(21090), "TCP port")
    ("-cpu,c", po::value<string>(&ctx.in_cpu_list), "CPU core list to pin threads")
    ;
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  } if (false == ValidateCmdArgs(ctx)) {
    return 2;
  }
  vector<int> cpu_cores = tick_calc::AvailableCpuCores(ctx.in_cpu_list);
  if (cpu_cores.size() > 1) {
    tick_calc::SetThreadCpuAffinity(cpu_cores[0]);
  }
  ctx.nbbo_data_manager = make_unique<tick_calc::RecordsetManager<Nbbo>>(ctx.in_data_dir);

  tick_calc::InitializeFunctionDefinitions();
  NetInitialize(ctx);
  tick_calc::CreateThreads(cpu_cores);
  signal(SIGTERM, ExitSignalHandler);
  try {
    while (!exit_signal.load()) {
        NetPoll(ctx);
    }
  }
  catch (exception& ex) {
    cerr << ex.what();
  }
  tick_calc::DestroyThreads();
  cout << " In " __FILE__ "\n";
  return retval;
}
