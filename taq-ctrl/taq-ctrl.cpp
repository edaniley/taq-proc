
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

string file_path, query_symbol;
bool pretty, sorted, no_header;

struct separate_thousands : numpunct<char> {
  char_type do_thousands_sep() const override { return ','; }
  string_type do_grouping() const override { return "\3"; }
};

string ToString(int value, int width) {
  ostringstream ss;
  ss.imbue(locale(""));
  ss << value;
  string str = ss.str();
  str.insert(str.begin(), width - str.size(), ' ');
  return str;
}

void HandleSecMasterFile(const FileHeader& , const mm::mapped_region & ) {//mm_region) {

}

void ShowRecords(const Nbbo* rec, const Nbbo* end) {
  cout << setprecision(4);
  do {
    cout << "time:" << rec->time
      << " bid:[ " << rec->bidp << " " << rec->bids
      << " ] offer: [" << rec->askp << " " << rec->asks
      << " ]" << endl;
  } while (++rec < end);
}

void ShowRecords(const NbboPrice *rec, const NbboPrice * end) {
  cout << setprecision(4);
  do {
    cout << "time:" << rec->time << " bid:" << rec->bidp << " offer:" << rec->askp << endl;
  } while (++rec < end);
}

void ShowSymbolRecords(const FileHeader& fh, const mm::mapped_region& mm_region, const SymbolMap* symbol_map) {
  const SymbolMap *symb = nullptr;
  for (int i = 0; i < fh.symb_cnt; i ++) {
    if (query_symbol == string(symbol_map[i].symb)) {
      symb = symbol_map + i;
      break;
    }
  }
  if (symb) {
    const int rec_cnt = symb->end - symb->start + 1;
    if (fh.type == RecordType::Nbbo) {
      const Nbbo* begin = (const Nbbo*)((char*)(mm_region.get_address()) + sizeof(fh) + (symb->start - 1) * sizeof(Nbbo));
      const Nbbo* end = begin + rec_cnt;
      ShowRecords(begin, end);
    } else if (fh.type == RecordType::NbboPrice) {
      const NbboPrice* begin = (const NbboPrice*)((char*)(mm_region.get_address()) + sizeof(fh) + (symb->start - 1) * sizeof(NbboPrice));
      const NbboPrice* end = begin + rec_cnt;
      ShowRecords(begin, end);
    }
  }
}

void HandleNbboFile(const FileHeader& fh, const mm::mapped_region & mm_region) {
  const size_t rec_size = fh.type == RecordType::Nbbo ? sizeof(Nbbo) : sizeof(NbboPrice);
  if ((sizeof(fh) + fh.symb_cnt * sizeof(SymbolMap) + fh.rec_cnt  * rec_size)  != mm_region.get_size()) {
    throw domain_error("Input file corruption : " + file_path);
  }
  if (false == no_header) {
    auto thousands = make_unique<separate_thousands>();
    auto saved_locale = cout.imbue(locale(cout.getloc(), thousands.release()));
    cout << "date file     " << file_path << endl;
    cout << "file size     " << mm_region.get_size() << endl;
    cout << "record type   " << (fh.type == RecordType::Nbbo ? "Nbbo (with size)" : "Nbbo (price only)") << endl;
    cout << "symbol count  " << fh.symb_cnt << endl;
    cout << "symbols" << endl;
    cout.imbue(saved_locale);
  }
  const SymbolMap* symbol_map =  (const SymbolMap * )((char *)(mm_region.get_address()) + sizeof(fh) + fh.rec_cnt * rec_size);
  vector<pair<string, int>> symbols;
  for (int i = 0; i < fh.symb_cnt; i++) {
    const SymbolMap& map = symbol_map[i];
    symbols.push_back(make_pair(map.symb, map.end - map.start + 1));
  }
  if (sorted) {
    sort(symbols.begin(), symbols.end(), [] (const auto & left, const auto& right) {return right.second < left.second;});
  }
  for (auto & symbol : symbols ) {
    if (pretty) {
      symbol.first.append(12 - symbol.first.length(), ' ');
      cout << symbol.first  << ToString(symbol.second, 10) << endl;
    } else {
      cout << symbol.first<< ","  << symbol.second << endl;
    }
  }
  if (! query_symbol.empty()) {
    ShowSymbolRecords(fh, mm_region, symbol_map);
  }
}


int main(int argc, char** argv) {
  int retval = 0;
  po::options_description desc("program options");
  desc.add_options()
    ("help,h", "produce help message")
    ("file,f", po::value<string>(&file_path), "path to datafile")
    ("pretty", po::bool_switch(&pretty)->default_value(false), "use pretty formatting")
    ("sort", po::bool_switch(&sorted)->default_value(false), "sort symbols by record count in descending order")
    ("no-header", po::bool_switch(&no_header)->default_value(false), "print output without summary header")
    ("symbol,s", po::value<string>(&query_symbol), "display records for specified symbol")
    ;
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  } catch (exception & ex) {
    cerr << ex.what() << endl << endl;
    cerr << desc << endl;
    return 2;
  }
  if (vm.count("help")) {
    cout << desc << endl;
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
    else if (fh.type == RecordType::Nbbo || fh.type == RecordType::NbboPrice) {
      HandleNbboFile(fh, mmreg);
    }
  }
  catch (const exception& ex) {
    cerr << ex.what() << endl;
    retval = 3;
  }
  return retval;
 }