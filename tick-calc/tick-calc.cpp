
#include <iostream>
#include <cassert>
#include <thread>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include "tick-calc.h"

using namespace std;
using namespace Taq;

namespace po = boost::program_options;
namespace fs = boost::filesystem;

static bool ValidateCmdArgs(tick_calc::AppContext & ctx) {
  if (false == (fs::exists(ctx.in_data_dir) && fs::is_directory(ctx.in_data_dir)) ) {
    cerr << "Invalid --data-dir: " << ctx.in_data_dir << endl;
    return false;
  }
  return true;
}

static vector<int> AvailableCpuCores(string &cpu_list) {
  vector<int> cores;
  vector<string> tokens;
  boost::split(tokens, cpu_list, boost::is_any_of(","));
  for (const string & token : tokens) {
    if (!token.empty()) {
      vector<string> range;
      boost::split(range, token, boost::is_any_of("-"));
      if (range.size() == 1) {
        cores.push_back(stoi(token.c_str()));
      }
      else if (range.size() == 2) {
        for (int core = stoi(range[0].c_str()); core <= stoi(range[1].c_str()); core++) {
          cores.push_back(core);
        }
      }
    }
  }
  int num_cores = thread::hardware_concurrency();
  auto it = remove_if(cores.begin(), cores.end(), [num_cores] (int core) {return core >= num_cores;});
  cores.resize(cores.size() - distance(it, cores.end()));
  sort(cores.begin(), cores.end(), less<int>());
  return vector<int>(cores.begin(), unique(cores.begin(), cores.end()));
}

bool SetThreadCpuAffinity(int core) {
#ifdef _MSC_VER
  bool success = SetThreadAffinityMask(GetCurrentThread(), (DWORD_PTR)1 << core) != 0;
#else
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  bool success = sched_setaffinity(0, sizeof(cpuset), &cpuset) == 0;
#endif
  return success;
}

static void test(tick_calc::AppContext & ctx) {
  try {
    NetInitialize(ctx);
    for (;;) {
      NetPoll(ctx);
    }
  } catch (exception &ex) {
    cerr << ex.what();
  }

  //tick_calc::RecordsetManager<Nbbo> quote_mgr(ctx.data_dir);
  //const tick_calc::SymbolRecordset<Nbbo>& rec = quote_mgr.LoadSymbolRecordset(MkTaqDate("20200331"), "AAPL");
  //vector<Nbbo> vv(rec.records.begin(), rec.records.begin() + rec.records.size());
  //assert (vv.size() == rec.records.size());
  //quote_mgr.UnloadSymbolRecordset(MkTaqDate("20200331"), "AAPL");

  //tick_calc::SecMasterManager sec_mgr(ctx.data_dir);
  //const tick_calc::SecMaster & sec31 = sec_mgr.Load(MkTaqDate("20200331"));
  //const tick_calc::SecMaster& sec30 = sec_mgr.Load(MkTaqDate("20200330"));
  //const tick_calc::SecMaster& sec31x = sec_mgr.Load(MkTaqDate("20200331"));
  //assert(&sec31x== &sec31);
  //sec_mgr.Release(sec31x);
  //sec_mgr.Release(sec31);
  //sec_mgr.Release(sec30);
  //const tick_calc::SecMaster& sec29 = sec_mgr.Load(MkTaqDate("20200329"));
  //assert(&sec29 != &sec30);
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
  ctx.cpu_cores = AvailableCpuCores(ctx.in_cpu_list);
  if (ctx.cpu_cores.size() > 1) {
    SetThreadCpuAffinity(ctx.cpu_cores[0]);
  }
  ctx.nbbo_data_manager = make_unique<tick_calc::RecordsetManager<Nbbo>>(ctx.in_data_dir);
  test(ctx);

  cout << " In " __FILE__ "\n";
  return retval;
}
