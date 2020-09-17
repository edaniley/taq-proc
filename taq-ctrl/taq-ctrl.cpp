
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
#include "boost-algorithm-string.h"
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

void HandleSecMasterFile(const FileHeader& fh , const mm::mapped_region& mm_region) {
  if (sizeof(fh) + fh.symb_cnt * sizeof(Security) != mm_region.get_size()) {
    throw domain_error("Input file corruption : " + file_path);
  }
  if (false == no_header) {
    auto thousands = make_unique<separate_thousands>();
    auto saved_locale = cout.imbue(locale(cout.getloc(), thousands.release()));
    cout << "data file     " << file_path << endl;
    cout << "file size     " << mm_region.get_size() << endl;
    cout << "record type   " "SecMaster" << endl;
    cout << "record size   " << sizeof(Security) << endl;
    cout << "symbol count  " << fh.symb_cnt << endl << endl;
    cout.imbue(saved_locale);
    if (pretty) {
      cout << "cta_symb    utp_symb    prim_exch tape lot_size" << endl;
    } else {
      cout << "cta_symb,utp_symb,prim_exch,tape,lot_size,"
      "volume_total,volume_regular,volume_trf,volume_block,volume_pre_open,volume_post_close,"
      "open_time_primary,open_price_primary,open_volume_primary,"
      "close_time_primary,close_price_primary,close_volume_primary,"
      "average_trade_size,min_price_regular,max_price_regular,vwap_regular"
      << endl;
    }
  }
  const Security* symbols = (const Security*)((char*)(mm_region.get_address()) + sizeof(fh));
  for (auto i = 0; i < fh.symb_cnt; i++) {
    const Security& sec = symbols[i];
    if (pretty) {
      string cta_symb(sec.symb); cta_symb.append(12 - cta_symb.size(), ' ');
      string utp_symb(sec.utp_symb); utp_symb.append(12 - utp_symb.size(), ' ');
      string exch(10, ' '); exch[0] = sec.exch;
      string tape(5, ' '); tape[0] = sec.tape;
      string lot_size = to_string((int)sec.lot_size); lot_size.insert(0, 8 - lot_size.size(), ' ');
      cout << cta_symb << utp_symb << exch << tape << lot_size << endl;
    } else {
      char buff[32];
      auto PrintPrice = [&buff](double px) {
        px  = px == numeric_limits<double>::max() ? .0 : px;
        sprintf_s(buff, sizeof(buff), "%.6f", px);
        return string(buff);
      };
      cout << sec.symb << "," << sec.utp_symb << "," << sec.exch << "," << sec.tape << "," << (int)sec.lot_size
        << "," << sec.volume_total << "," << sec.volume_regular << "," << sec.volume_trf << "," << sec.volume_block
        << "," << sec.volume_pre_open << "," << sec.volume_post_close
        << "," << sec.open_time_primary << "," << PrintPrice(sec.open_price_primary) << "," << sec.open_volume_primary
        << "," << sec.close_time_primary << "," << PrintPrice(sec.close_price_primary) << "," << sec.close_volume_primary
        << "," << sec.average_trade_size << "," << PrintPrice(sec.min_price_regular) << "," << PrintPrice(sec.max_price_regular)
        << "," << PrintPrice(sec.vwap_regular)<< endl;
    }
  }
}

void ShowRecords(const string & symb, const Nbbo* rec, const Nbbo* end) {
  cout << setprecision(4);
  do {
    cout << "symbol:" << symb << " time:" << rec->time
      << " bid:[ " << rec->bidp << " " << rec->bids
      << " ] offer: [" << rec->askp << " " << rec->asks
      << " ]" << endl;
  } while (++rec < end);
}

void ShowRecords(const string& symb, const NbboPrice *rec, const NbboPrice * end) {
  cout << setprecision(4);
  do {
    cout << "symbol:" << symb << " time:" << rec->time << " bid:" << rec->bidp << " offer:" << rec->askp << endl;
  } while (++rec < end);
}

void ShowRecords(const string& symb, const Trade* rec, const Trade* end) {
  do {
    if (pretty) {
      cout << "symbol:" << symb << " time:" << rec->time << " price:" << rec->price << " qty:" << rec->qty
        << " exch:" << (char)rec->attr.exch << " cond:'" << string(rec->cond, sizeof(rec->cond)) << "' trf:" << (char)rec->attr.exch
        << "' lte:" << (rec->attr.lte ? 'Y' : 'N') << " ve:" << (rec->attr.ve ? 'Y' : 'N')
        << " vwap:" << (rec->attr.vwap_eligible? 'Y' : 'N')  << " trf:" << (rec->attr.trf ? 'Y' : 'N')
        << " block:" << (rec->attr.block_trade ? 'Y' : 'N')  << endl;
    } else {
      cout << symb << ',' << rec->time << ',' << rec->price << ',' << rec->qty
        << ',' << (char)rec->attr.exch << ',' << (char)rec->attr.exch << ',' << string(rec->cond, sizeof(rec->cond))
        << ',' << (rec->attr.lte ? 'Y' : 'N') << ',' << (rec->attr.ve ? 'Y' : 'N')
        << ',' << (rec->attr.vwap_eligible ? 'Y' : 'N') << ',' << (rec->attr.trf ? 'Y' : 'N')
        << ',' << (rec->attr.block_trade ? 'Y' : 'N') << endl;

    }
  } while (++rec < end);
}

void ShowSymbolRecords(const FileHeader& fh, const mm::mapped_region& mm_region, const SymbolMap* symbol_map) {
  vector<string> symbol_list;
  boost::split(symbol_list, query_symbol, boost::is_any_of(","));
  for (const string & symbol: symbol_list) {
    const SymbolMap *symb = nullptr;
    for (int i = 0; i < fh.symb_cnt; i ++) {
      if (symbol == string(symbol_map[i].symb)) {
        symb = symbol_map + i;
        break;
      }
    }
    if (symb) {
      const int rec_cnt = symb->end - symb->start + 1;
      if (fh.type == RecordType::Nbbo) {
        const Nbbo* begin = (const Nbbo*)((char*)(mm_region.get_address()) + sizeof(fh) + (symb->start - 1) * sizeof(Nbbo));
        const Nbbo* end = begin + rec_cnt;
        ShowRecords(symbol, begin, end);
      } else if (fh.type == RecordType::NbboPrice) {
        const NbboPrice* begin = (const NbboPrice*)((char*)(mm_region.get_address()) + sizeof(fh) + (symb->start - 1) * sizeof(NbboPrice));
        const NbboPrice* end = begin + rec_cnt;
        ShowRecords(symbol, begin, end);
      } else if (fh.type == RecordType::Trade) {
        const Trade* begin = (const Trade*)((char*)(mm_region.get_address()) + sizeof(fh) + (symb->start - 1) * sizeof(Trade));
        const Trade* end = begin + rec_cnt;
        ShowRecords(symbol, begin, end);
      }
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
    cout << "data file     " << file_path << endl;
    cout << "file size     " << mm_region.get_size() << endl;
    cout << "record type   " << (fh.type == RecordType::Nbbo ? "Nbbo (with size)" : "Nbbo (price only)") << endl;
    cout << "record size   " << (fh.type == RecordType::Nbbo ? sizeof(Nbbo) : sizeof(NbboPrice)) << endl;
    cout << "symbol count  " << fh.symb_cnt << endl << endl;
    cout.imbue(saved_locale);
  }
  const SymbolMap* symbol_map = (const SymbolMap*)((char*)(mm_region.get_address()) + sizeof(fh) + fh.rec_cnt * rec_size);
  if (query_symbol.empty()) {
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
  } else {
    ShowSymbolRecords(fh, mm_region, symbol_map);
  }
}

void HandleTradeFile(const FileHeader& fh, const mm::mapped_region& mm_region) {
  if ((sizeof(fh) + fh.symb_cnt * sizeof(SymbolMap) + fh.rec_cnt * sizeof(Trade)) != mm_region.get_size()) {
    throw domain_error("Input file corruption : " + file_path);
  }
  if (false == no_header) {
    auto thousands = make_unique<separate_thousands>();
    auto saved_locale = cout.imbue(locale(cout.getloc(), thousands.release()));
    cout << "data file     " << file_path << endl;
    cout << "file size     " << mm_region.get_size() << endl;
    cout << "record type   " "Trade" << endl;
    cout << "record type   " << sizeof(Trade) << endl;
    cout << "symbol count  " << fh.symb_cnt << endl << endl;
    cout.imbue(saved_locale);
  }
  const SymbolMap* symbol_map = (const SymbolMap*)((char*)(mm_region.get_address()) + sizeof(fh) + fh.rec_cnt * sizeof(Trade));
  if (query_symbol.empty()) {
    vector<pair<string, int>> symbols;
    for (int i = 0; i < fh.symb_cnt; i++) {
      const SymbolMap& map = symbol_map[i];
      symbols.push_back(make_pair(map.symb, map.end - map.start + 1));
    }
    if (sorted) {
      sort(symbols.begin(), symbols.end(), [](const auto& left, const auto& right) {return right.second < left.second; });
    }
    for (auto& symbol : symbols) {
      if (pretty) {
        symbol.first.append(12 - symbol.first.length(), ' ');
        cout << symbol.first << ToString(symbol.second, 10) << endl;
      }
      else {
        cout << symbol.first << "," << symbol.second << endl;
      }
    }
  } else {
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
    ("no-header", po::bool_switch(&no_header)->default_value(false), "print output without header information")
    ("symbols,s", po::value<string>(&query_symbol), "comma-separated symbol list")
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
    else if (fh.type == RecordType::Trade) {
      HandleTradeFile(fh, mmreg);
    }
  }
  catch (const exception& ex) {
    cerr << ex.what() << endl;
    retval = 3;
  }
  return retval;
 }