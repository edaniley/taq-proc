

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "taq-prep.h"

using namespace std;
namespace dt = boost::date_time;

enum Exchange {
  Exch_A = 'A' - 'A' ,// NYSE American,LLC (NYSE American)
  Exch_B = 'B' - 'A' ,// NASDAQ OMX BX,Inc. (NASDAQ OMX BX)
  Exch_C = 'C' - 'A' ,// NYSE National,Inc. (NYSE National)
  Exch_D = 'D' - 'A' ,// FINRA Alternative Display Facility (ADF)
  Exch_I = 'I' - 'A' ,// International Securities Exchange, LLC (ISE)
  Exch_J = 'J' - 'A' ,// Cboe EDGA Exchange (Cboe EDGA)
  Exch_K = 'K' - 'A' ,// Cboe EDGX Exchange (Cboe EDGX)
  Exch_L = 'L' - 'A' ,// Long-Term Stock Exchange,Inc. (LTSE)
  Exch_M = 'M' - 'A' ,// Chicago Stock Exchange,Inc. (NYSEChicago)
  Exch_N = 'N' - 'A' ,// New York Stock Exchange,LLC (NYSE)
  Exch_P = 'P' - 'A' ,// NYS EArca,Inc. (NYSEA rca)
  Exch_S = 'S' - 'A' ,// Consolidated Tape System (CTS)
  Exch_T = 'T' - 'A' ,// NASDAQ Stock Market,LLC (in Tape A,B securities)(NASDAQ)
  Exch_Q = 'Q' - 'A' ,// NASDAQ Stock Exchange,LLC (in Tape C securities)
  Exch_V = 'V' - 'A' ,// The Investors’ Exchange,LLC (IEX)
  Exch_W = 'W' - 'A' ,// Chicago Broad Options Exchange,Inc. (CBSX)
  Exch_X = 'X' - 'A' ,// NASDAQ OMX PSX,Inc. (NASDAQ OMX PSX)
  Exch_Y = 'Y' - 'A' ,// Cboe BYX Exchange,Inc (Cboe BYX)
  Exch_Z = 'Z' - 'A' ,// Cboe BZX Exchange,Inc (Cboe BZX)
  Exch_Max
};


enum QuoteColum {
  QCOL_Time ,
  QCOL_Exchange ,
  QCOL_Symbol ,
  QCOL_Bid_Price ,
  QCOL_Bid_Size ,
  QCOL_Offer_Price ,
  QCOL_Offer_Size ,
  QCOL_Quote_Condition ,
  QCOL_Sequence_Number ,
  QCOL_National_BBO_Ind ,
  QCOL_FINRA_BBO_Indicator ,
  QCOL_FINRA_ADF_MPID_Indicator ,
  QCOL_Quote_Cancel_Correction ,
  QCOL_Source_Of_Quote ,
  QCOL_Retail_Interest_Indicator ,
  QCOL_Short_Sale_Restriction_Indicator ,
  QCOL_LULD_BBO_Indicator ,
  QCOL_SIP_Generated_Message_Identifier ,
  QCOL_National_BBO_LULD_Indicator ,
  QCOL_Participant_Timestamp ,
  QCOL_FINRA_ADF_Timestamp ,
  QCOL_FINRA_ADF_Market_Participant_Quote_Indicator ,
  QCOL_Security_Status_Indicator ,
  QCOL_Max
};

namespace taq_prep {

struct Bbo {
  double bprice;
  double oprice;
  int bsize;
  int osize;
  bool bvalid;
  bool ovalid;
  Bbo(const vector<string> & row) :
    bprice(stod(row[QCOL_Bid_Price])),
    oprice(stod(row[QCOL_Offer_Price])),
    bsize(stoi(row[QCOL_Bid_Size])),
    osize(stoi(row[QCOL_Offer_Size])),
    bvalid(false), ovalid(false) {
  }
private:
  Bbo();
};

struct Nbbo {
  double bprice;
  double oprice;
  int bsize;
  int osize;
};


static bool ValidateQuote(const vector<string> & row, Bbo & bbo) {
    const char src = row[QCOL_Source_Of_Quote][0];
    const char cond = row[QCOL_Quote_Condition][0];
    if (src == 'C') {
      static string invalid_cta_condition_codes("CLNU4");
      const bool valid = invalid_cta_condition_codes.find(cond) == string::npos;
      bbo.bvalid = valid && cond != 'E' && bbo.bsize > 0;
      bbo.ovalid = valid && cond != 'F' && bbo.osize > 0;
    } else if (src == 'N') {
      static string invalid_utp_condition_codes("FILNUXZ4");
      const bool valid = invalid_utp_condition_codes.find(cond) == string::npos;
      bbo.bvalid = valid && bbo.bsize > 0 ;
      bbo.ovalid = valid && bbo.osize > 0;
    } else {
      throw(domain_error("Unknown source" + src));
    }
    return bbo.bvalid || bbo.ovalid;
}

static boost::posix_time::time_duration MkTime(const string & timestamp) {
  return  boost::posix_time::hours(                 stoi(timestamp.substr(0,2)) )
        + boost::posix_time::minutes(               stoi(timestamp.substr(2,2)) )
        + boost::posix_time::seconds(               stoi(timestamp.substr(4,2)) )
        + boost::posix_time::microseconds( lround ( stod(timestamp.substr(6,9)) / 1000) );
}

static void HandleQuote(const string & timestamp, const string & symbol, const string & exchange, Bbo & bbo) {
  boost::posix_time::time_duration time_of_day = MkTime(timestamp);
  (void) time_of_day;
  (void) symbol;
  (void) exchange;
  (void) bbo;
}

int ProcessQuotes(AppContext & ctx, istream & is) {
      while(!is.eof()) {
        string line;
        getline(is, line);
        vector<string> row;
        boost::split(row, line, boost::is_any_of("|"));
        if (row[QCOL_Time] == "Time")
          continue;
        if (row.size() != QCOL_Max) {
          char msg[128];
          sprintf(msg, "Input record size mismatch: expected %d columns, received %d", QCOL_Max, (int)row.size());
          throw(domain_error(msg));
        }
        if (row[QCOL_Time].size() != 15) {
          throw(domain_error("Unrecognized Time format"));
        }
        Bbo bbo(row);
        if (ValidateQuote(row, bbo)) {
          HandleQuote(row[QCOL_Time], row[QCOL_Symbol], row[QCOL_Exchange], bbo);
        }
    }
    return 0;
}

}
