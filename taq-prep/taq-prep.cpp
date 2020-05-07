

#include <iostream>
#include <fstream>
#include <numeric>
#include <algorithm>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

using namespace std;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

int ProcessInputStream(istream & is) {
    while(!is.eof()) {
        string line;
        getline(is, line);
        cout << line << endl;
    }
    return 0;
}

int ProcessFiles(vector<string> &input_files , const string & ) {
    int retval = 0;
    for(const auto & file : input_files) {
        std::ifstream is(file, ifstream::in);
        retval += ProcessInputStream(is);
    }
    return 0;
}

int main(int argc, char **argv) {
    int retval = 0;
    vector<string> in_files;
    string input_type, out_dir;
    po::options_description desc("program options");
    desc.add_options()
      ("help,h", "produce help message")
      ("in-files,i", po::value<vector<string>>(&in_files)->multitoken(), "space-separated list of input files")
      ("in-type,t", po::value<string>(&input_type)->default_value("quote"), "input file type (master, quote, trade)")
      ("out-dir,o", po::value<string>(&out_dir)->default_value("."), "output directory")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    if (vm.count("help")) {
        cout << desc << endl;
        return 1;
    }
    if (fs::exists(out_dir) && fs::is_directory(out_dir)) {
        if (in_files.size()) {
            size_t valid_files = 0;
            for_each(in_files.begin(), in_files.end(), [&] (const string &path) {
                valid_files += fs::exists(path) && fs::is_regular(path) ? 1 : (cerr << "Invalid path: " << path << endl, 0);
            });
            retval = in_files.size() == valid_files ? ProcessFiles(in_files, out_dir ) : 2;
        } else {
            retval = ProcessInputStream(cin);
        }
    } else {
        cerr << "Invalid output directory: " << out_dir << endl;
        retval = 3;
    }
    return retval;
}
