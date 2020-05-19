#ifndef TAQ_POC_INCLUDED
#define TAQ_POC_INCLUDED

#include "taq-proc.h"
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

typedef char Symbol[16];

struct SymbolMap {
  Symbol symb;
  size_t start;
  size_t end;
  SymbolMap() = delete;
  SymbolMap(const std::string &symbol, size_t start, size_t end) : start(start), end(end) {
    std::memset(symb, sizeof(symb), 0);
    std::strncpy(symb, symbol.c_str(), sizeof(symb) - 1);
  }
};

struct FileHeader {
  enum Type {
    SecMaster, Nbbo, Trade
  };
  const size_t size;
  const Type type;
  const int version;
  size_t symb_cnt;
  FileHeader() = delete;
  FileHeader(FileHeader::Type type, int version) : size(sizeof(FileHeader)), type(type), version(version), symb_cnt(0) { }
};

struct Nbbo {
  const boost::posix_time::time_duration time;
  const double bidp;
  const double aidp;
  const int bids;
  const int aids;
  Nbbo() = delete;
  Nbbo(const boost::posix_time::time_duration & time, double bidp, double aidp, int bids, int aids)
    : time(time), bidp(bidp), aidp(aidp), bids(bids), aids(aids) {}
};

}

#endif
