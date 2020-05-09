

#include <numeric>
#include <algorithm>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include "taq-prep.h"

using namespace std;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

int ProcessInputStream(taq_prep::AppContext &ctx, istream & is) {
    if (ctx.input_type == "master") {
        return taq_prep::ProcessSecMaster(ctx, is);
    } else if (ctx.input_type == "quote") {
        return taq_prep::ProcessQuotes(ctx, is);
    } else if (ctx.input_type == "trade") {
        return taq_prep::ProcessTrades(ctx, is);
    }
    return 0;
}

int ProcessFiles(taq_prep::AppContext &ctx){//vector<string> &input_files , const string & ) {
    int retval = 0;
    for(const auto & file : ctx.input_files) {
        std::ifstream is(file, ifstream::in);
        retval += ProcessInputStream(ctx, is);
    }
    return retval;
}

int main(int argc, char **argv) {
    int retval = 0;
    taq_prep::AppContext ctx;
    po::options_description desc("program options");
    desc.add_options()
      ("help,h", "produce help message")
      ("date,d", po::value<string>(&ctx.date)->default_value(""), "trade date")
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
    }

    bool valid = fs::exists(ctx.output_dir) && fs::is_directory(ctx.output_dir);
    if (false == valid) {
        cerr << "Invalid --out-dir: " << ctx.output_dir << endl;
    }
    if (valid && ctx.input_files.size()) {
        size_t valid_files = 0;
        for_each(ctx.input_files.begin(), ctx.input_files.end(), [&] (const string &path) {
            valid_files += fs::exists(path) && fs::is_regular(path) ? 1 : (cerr << "Invalid path: " << path << endl, 0);
        });
        retval = ctx.input_files.size() == valid_files ? ProcessFiles(ctx) : 2;
    } else if (valid) {
        const bool valid_type = ctx.input_type == "master" || ctx.input_type == "quote" || ctx.input_type == "trade";
        const bool valid = ctx.date.size() == 8 && valid_type;
        if (valid) {
            retval = ProcessInputStream(ctx, cin);
        } else {
            retval = 2;
            if (ctx.date.empty()) {
                cerr << "--date required for stdin" << endl;
            } else if (ctx.date.size() != 8) {
                cerr << "Invalid --date format" << endl;
            } else if (ctx.input_type.empty()) {
                cerr << "--in-type required for stdin" << endl;
            } else if (false == valid_type) {
                cerr << "Invalid --in-type value:" << ctx.input_type << endl;
            }
        }
    }
    return retval;
}
