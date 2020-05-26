
#include <iostream>
#include <cassert>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include "tick-calc.h"

using namespace std;
using namespace Taq;

namespace po = boost::program_options;
namespace fs = boost::filesystem;

static bool ValidateCmdArgs(tick_calc::AppContext & ctx) {
  if (false == (fs::exists(ctx.data_dir) && fs::is_directory(ctx.data_dir)) ) {
    cerr << "Invalid --data-dir: " << ctx.data_dir << endl;
    return false;
  }
  return true;
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
    ("data-dir,d", po::value<string>(&ctx.data_dir)->default_value("."), "output directory")
    ("-tcp,t", po::value<uint16_t>(&ctx.port)->default_value(21090), "TCP port")
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
  test(ctx);

  std::cout << " In " __FILE__ "\n";
  return retval;
}
