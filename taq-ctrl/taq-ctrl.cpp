
#include <iostream>
#include <exception>
#include <sstream>
#include <vector>
#include <algorithm>
#include <locale>
#include <memory>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "taq-proc.h"

using namespace std;
using namespace Taq;

namespace po = boost::program_options;
namespace fs = boost::filesystem;
namespace mm = boost::interprocess;

string file_path;

struct separate_thousands : numpunct<char> {
  char_type do_thousands_sep() const override { return ','; }
  string_type do_grouping() const override { return "\3"; }
};

string ToString(int value, int width) {
  ostringstream ss;
  ss.imbue(locale(""));
  ss << value;
  string str = ss.str();
  str.insert(str.begin(), 10 - str.size(), ' ');
  return str;
}

void HandleSecMasterFile(const FileHeader& , const mm::mapped_region & ) {//mm_region) {

}

void HandleNbboFile(const FileHeader& fh, const mm::mapped_region & mm_region) {
  if ((sizeof(fh) + fh.symb_cnt * sizeof(SymbolMap) + fh.rec_cnt  * sizeof(Nbbo))  != mm_region.get_size()) {
    throw domain_error("Input file corruption : " + file_path);
  }
  auto thousands = make_unique<separate_thousands>();
  cout.imbue(locale(cout.getloc(), thousands.release()));
  cout << "date file     " << file_path << endl;
  cout << "file size     " << mm_region.get_size() << endl;
  cout << "record type   " << "Nbbo" << endl;
  cout << "symbol count  " << fh.symb_cnt << endl;
  cout << "symbols" << endl;
  const SymbolMap* symbols =  (const SymbolMap * )((char *)(mm_region.get_address()) + sizeof(fh) + fh.rec_cnt * sizeof(Nbbo));
  for (int i = 0; i < fh.symb_cnt; i++) {
    const SymbolMap& map = symbols[i];
    string symbol = map.symb;
    symbol.append(12 - symbol.length(), ' ');
    cout << symbol << ToString(map.end - map.start + 1, 10) << endl;
  }
}

int main(int argc, char** argv) {
  int retval = 0;
  po::options_description desc("program options");
  desc.add_options()
    ("help,h", "produce help message")
    ("file,f", po::value<string>(&file_path), "path to datafile")
    ;
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  if (vm.count("help")) {
    return 1;
  }
  try {
    if (false == (fs::exists(file_path) && fs::is_regular_file(file_path))) {
      throw domain_error("Input file not found : " + file_path);
    }
    const size_t file_size = (size_t)fs::file_size(file_path);
    if (file_size < sizeof(FileHeader)) {
      throw domain_error("Input file size too small to accomodate header : " + file_path);
    }
    mm::file_mapping mmfile(file_path.c_str(), mm::read_only);
    mm::mapped_region mmreg(mmfile, mm::read_only);
    const FileHeader& fh = *(FileHeader*)mmreg.get_address();
    if (fh.type == RecordType::SecMaster) {
      HandleSecMasterFile(fh, mmreg);
    }
    else if (fh.type == RecordType::Nbbo) {
      HandleNbboFile(fh, mmreg);
    }
  }
  catch (const exception& ex) {
    cerr << ex.what() << endl;
    retval = 3;
  }
  return retval;
 }