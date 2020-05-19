

#include <numeric>
#include <algorithm>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include "taq-prep.h"

using namespace std;
using namespace Taq;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

static void OpenOutputStream(taq_prep::AppContext& ctx) {
  ostringstream ss;
  fs::path out_path(ctx.output_dir);
  if (ctx.input_type == "master") {
    ss << ctx.date << ".sec-master." << ".dat";
  }
  else if (ctx.input_type == "quote") {
    ss << ctx.date << ".quotes." << ctx.symb << ".dat";
  }
  else if (ctx.input_type == "trade") {
    ss << ctx.date << ".trades." << ctx.symb << ".dat";
  }
  out_path /= ss.str();
  if (fs::exists(out_path) && fs::is_regular_file(out_path)) {
    fs::remove(out_path);
  }
  ctx.output.open(out_path.string(), ios::out | ios::binary);
}
static void CloseOutputStream(taq_prep::AppContext& ctx) {
  ctx.output.close();
}
/* ===================================================== page ========================================================*/
static int ProcessInputStream(taq_prep::AppContext& ctx, istream& is) {
  if (ctx.input_type == "master") {
    return taq_prep::ProcessSecMaster(ctx, is);
  }
  else if (ctx.input_type == "quote") {
    return taq_prep::ProcessQuotes(ctx, is);
  }
  else if (ctx.input_type == "trade") {
    return taq_prep::ProcessTrades(ctx, is);
  }
  return 0;
}
/* ===================================================== page ========================================================*/
static int ProcessFiles(taq_prep::AppContext &ctx) {
  int retval = 0;
  for(const auto & file : ctx.input_files) {
      std::ifstream is(file, ifstream::in);
      retval += ProcessInputStream(ctx, is);
  }
  return retval;
}
/* ===================================================== page ========================================================*/
static bool ValidateCmdArgs(taq_prep::AppContext & ctx) {
  if (false == fs::exists(ctx.output_dir) && fs::is_directory(ctx.output_dir)) {
    cerr << "Invalid --out-dir: " << ctx.output_dir << endl;
    return false;
  }
  if (ctx.input_files.size()) {
    size_t valid_files = 0;
    for_each(ctx.input_files.begin(), ctx.input_files.end(), [&](const string& path) {
      valid_files += fs::exists(path) && fs::is_regular(path) ? 1 : (cerr << "Invalid input path: " << path << endl, 0);
      });
    if (ctx.input_files.size() != valid_files) {
      return false;
    }
  }
  if (ctx.date.empty()) {
    cerr << "--date required for stdin" << endl;
    return false;
  }
  if (ctx.input_type != "master" && ctx.input_type != "quote" && ctx.input_type != "trade") {
      cerr << "Invalid --in-type:" << ctx.input_type << endl;
      return false;
  }
  if (ctx.input_type != "master" && ctx.symb.empty()) {
    cerr << "--symbol-group required for stdin" << endl;
    return false;
  }
  if (ctx.input_type != "master" && ctx.symb.size() > 1) {
    cerr << "Invalid --symbol-group:" << ctx.symb << endl;
    return false;
  }
  return true;
}
/* ===================================================== page ========================================================*/
int main(int argc, char **argv) {
  int retval = 0;
  taq_prep::AppContext ctx;
  po::options_description desc("program options");
  desc.add_options()
    ("help,h", "produce help message")
    ("date,d", po::value<string>(&ctx.date)->default_value(""), "trade date")
    ("symbol-group,s", po::value<string>(&ctx.symb)->default_value(""), "symbol group")
    ("in-files,i", po::value<vector<string>>(&ctx.input_files)->multitoken(), "space-separated list of input files")
    ("in-type,t", po::value<string>(&ctx.input_type)->default_value("quote"), "input file type (master, quote, trade)")
    ("out-dir,o", po::value<string>(&ctx.output_dir)->default_value("."), "output directory")
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
  try {
    OpenOutputStream(ctx);
    retval = ctx.input_files.size() ? ProcessFiles(ctx) : ProcessInputStream(ctx, cin);
    CloseOutputStream(ctx);
  } catch (const exception & ex) {
    cerr << ex.what() << endl;
    retval = 3;
  }
  return retval;
}
