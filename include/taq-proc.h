#ifndef TAQ_POC_INCLUDED
#define TAQ_POC_INCLUDED

#include "taq-proc.h"

namespace taq_proc {

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

}

#endif
