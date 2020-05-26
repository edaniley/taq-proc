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

enum class RecordType {
  NA, SecMaster, Nbbo, Trade
};

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
  const double askp;
  const int bids;
  const int asks;
  Nbbo(const Time& time, double bidp, double askp, int bids, int asks)
    : time(time), bidp(bidp), askp(askp), bids(bids), asks(asks) {}
};

struct Trade {
  const Time time;
  const double price;
  const int qty;
  char cond[4];
  Trade(const Time& trd_time, double trd_price, int trd_qty, char trd_cond[4])
    : time(trd_time), price(trd_price), qty(trd_qty) {
    memcpy(cond, trd_cond, 4);
  }
};

struct SymbolMap {
  Symbol symb;
  size_t start;
  size_t end;
  SymbolMap(const std::string &symbol, size_t start, size_t end) : start(start), end(end) {
    ::strncpy_s(symb, sizeof(symb), symbol.c_str(), sizeof(symb) - 1);
  }
};

struct FileHeader {
  const size_t size;
  RecordType type;
  const int version;
  const Date date;
  size_t symb_cnt;
  size_t rec_cnt;
  FileHeader(int version) : size(sizeof(FileHeader)), type(RecordType::NA), version(version), symb_cnt(0), rec_cnt(0) { }
};


inline RecordType RecordTypeFromString(const std::string type_name) {
  if (type_name == typeid(Security).name()) {
    return RecordType::SecMaster;
  } else if (type_name == typeid(Nbbo).name()) {
    return RecordType::Nbbo;
  } else if (type_name == typeid(Trade).name()) {
    return RecordType::Trade;
  } else if (type_name == typeid(Trade).name()) {
    return RecordType::Trade;
  } else if (type_name == "master") {
    return RecordType::SecMaster;
  } else if (type_name == "quote") {
    return RecordType::Nbbo;
  } else if (type_name == "trade") {
    return RecordType::Trade;
  } else {
    return RecordType::NA;
  }
}

inline
boost::filesystem::path MkDataFilePath(const std::string& data_dir, RecordType type, Date date, char symbol_group = 0) {
  std::ostringstream ss;
  boost::filesystem::path file_path(data_dir);
  const std::string yyyymmdd = boost::gregorian::to_iso_string(date);
  if (type == RecordType::SecMaster) {
    ss << yyyymmdd << ".sec-master" << ".dat";
  }
  else if (type == RecordType::Nbbo) {
    ss << yyyymmdd << ".quotes." << symbol_group << ".dat";
  }
  else if (type == RecordType::Trade) {
    ss << yyyymmdd << ".trades." << symbol_group << ".dat";
  }
  file_path /= ss.str();
  if (false == boost::filesystem::exists(file_path) && boost::filesystem::is_regular_file(file_path)) {
    throw std::domain_error("Input file not found : " + file_path.string());
  }
  return std::move(file_path);
}

}

#endif
