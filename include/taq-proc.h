#ifndef TAQ_PROC_INCLUDED
#define TAQ_PROC_INCLUDED


#include <bitset>
#include <string>
#include <exception>
#include <boost/filesystem.hpp>

#include "taq-time.h"

namespace Taq{

enum Exchange {
  Exch_A = 'A' - 'A',// NYSE American,LLC (NYSE American)
  Exch_B = 'B' - 'A',// NASDAQ OMX BX,Inc. (NASDAQ OMX BX)
  Exch_C = 'C' - 'A',// NYSE National,Inc. (NYSE National)
  Exch_D = 'D' - 'A',// FINRA Alternative Display Facility (ADF)
  Exch_I = 'I' - 'A',// International Securities Exchange, LLC (ISE)
  Exch_J = 'J' - 'A',// Cboe EDGA Exchange (Cboe EDGA)
  Exch_K = 'K' - 'A',// Cboe EDGX Exchange (Cboe EDGX)
  Exch_L = 'L' - 'A',// Long-Term Stock Exchange,Inc. (LTSE)
  Exch_M = 'M' - 'A',// Chicago Stock Exchange,Inc. (NYSEChicago)
  Exch_N = 'N' - 'A',// New York Stock Exchange,LLC (NYSE)
  Exch_P = 'P' - 'A',// NYS EArca,Inc. (NYSEA rca)
  Exch_S = 'S' - 'A',// Consolidated Tape System (CTS)
  Exch_T = 'T' - 'A',// NASDAQ Stock Market,LLC (in Tape A,B securities)(NASDAQ)
  Exch_Q = 'Q' - 'A',// NASDAQ Stock Exchange,LLC (in Tape C securities)
  Exch_V = 'V' - 'A',// The Investors’ Exchange,LLC (IEX)
  Exch_W = 'W' - 'A',// Chicago Broad Options Exchange,Inc. (CBSX)
  Exch_X = 'X' - 'A',// NASDAQ OMX PSX,Inc. (NASDAQ OMX PSX)
  Exch_Y = 'Y' - 'A',// Cboe BYX Exchange,Inc (Cboe BYX)
  Exch_Z = 'Z' - 'A',// Cboe BZX Exchange,Inc (Cboe BZX)
  Exch_Max
};

typedef char Symbol[18];
typedef std::bitset<Exch_Max> ExchangeMask;

struct Security {
  Symbol symb;
  char CUSIP[10];
  char type[4];
  Symbol sip_symb;
  Symbol prev_symb;
  char test_flag;
  char exch;
  char tape;
  uint8_t trd_unit;
  uint8_t lot_size;
  ExchangeMask exch_mask;
  char industry_code[5];
  char halt_reason;
  double shares_outstanding_m;
  Date eff_date;
};

struct Nbbo {
  const Time time;
  const double bidp;
  const double aidp;
  const int bids;
  const int aids;
  Nbbo() = delete;
  Nbbo(const Time& time, double bidp, double aidp, int bids, int aids)
    : time(time), bidp(bidp), aidp(aidp), bids(bids), aids(aids) {}
};

struct SymbolMap {
  Symbol symb;
  size_t start;
  size_t end;
  SymbolMap() = delete;
  SymbolMap(const std::string &symbol, size_t start, size_t end) : start(start), end(end) {
    std::memset(symb, 0, sizeof(symb));
    std::strncpy(symb, symbol.c_str(), sizeof(symb) - 1);
  }
};

struct FileHeader {
  enum class Type {
    SecMaster, Nbbo, Trade
  };
  const size_t size;
  Type type;
  const int version;
  size_t symb_cnt;
  size_t rec_cnt;
  FileHeader() = delete;
  FileHeader(int version) : size(sizeof(FileHeader)), version(version), symb_cnt(0), rec_cnt(0) { }
};

inline
boost::filesystem::path MkDataFilePath(const std::string& data_dir, FileHeader::Type type, Date date, char symbol_group = 0) {
  std::ostringstream ss;
  boost::filesystem::path file_path(data_dir);
  const std::string yyyymmdd = boost::gregorian::to_iso_string(date);
  if (type == FileHeader::Type::SecMaster) {
    ss << yyyymmdd << ".sec-master" << ".dat";
  }
  else if (type == FileHeader::Type::Nbbo) {
    ss << yyyymmdd << ".quotes" << symbol_group << ".dat";
  }
  else if (type == FileHeader::Type::Trade) {
    ss << yyyymmdd << ".trades" << symbol_group << ".dat";
  }
  file_path /= ss.str();
  if (false == boost::filesystem::exists(file_path) && boost::filesystem::is_regular_file(file_path)) {
    throw std::domain_error("Input file not found : " + file_path.string());
  }
  return std::move(file_path);
}

inline
boost::filesystem::path MkDataFilePath(const std::string& data_dir, const std::string file_type, Date date, char symbol_group = 0) {
  if (file_type == "master") {
    return MkDataFilePath(data_dir, FileHeader::Type::SecMaster, date, symbol_group);
  }
  else if (file_type == "quote") {
    return MkDataFilePath(data_dir, FileHeader::Type::SecMaster, date, symbol_group);
  }
  else if (file_type == "trade") {
    return MkDataFilePath(data_dir, FileHeader::Type::SecMaster, date, symbol_group);
  } else {
    return boost::filesystem::path(data_dir);
  }
}

}

#endif
