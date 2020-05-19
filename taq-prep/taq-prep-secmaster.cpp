

#include <algorithm>
#include <boost/algorithm/string.hpp>

#include "taq-prep.h"

using namespace std;
using namespace Taq;

namespace taq_prep {

int ProcessSecMaster(AppContext & ctx, istream & is) {
      while(!is.eof()) {
        string line;
        getline(is, line);
        vector<string> row;
        boost::split(row, line, boost::is_any_of("|"));
        if (row[MCOL_Symbol] == "Symbol")
          continue;

        Security sec;
        std::memset(&sec, 0, sizeof(sec));

        std::strncpy(sec.symb, row[MCOL_Symbol].c_str(), sizeof(sec.symb)-1);
        std::strncpy(sec.CUSIP, row[MCOL_CUSIP].c_str(), sizeof(sec.CUSIP)-1);
        std::strncpy(sec.type, row[MCOL_Security_Type].c_str(), sizeof(sec.type)-1);
        std::strncpy(sec.sip_symb, row[MCOL_SIP_Symbol].c_str(), sizeof(sec.sip_symb)-1);
        std::strncpy(sec.prev_symb, row[MCOL_Old_Symbol].c_str(), sizeof(sec.prev_symb)-1);
        std::strncpy(sec.industry_code, row[MCOL_NYSE_Industry_Code].c_str(), sizeof(sec.industry_code) - 1);

        sec.test_flag = row[MCOL_Test_Symbol_Flag][0];
        sec.exch = row[MCOL_Listed_Exchange][0];
        sec.tape = row[MCOL_Tape][0];
        sec.halt_reason = row[MCOL_Halt_Delay_Reason][0];

        sec.trd_unit = (uint8_t)stoi(row[MCOL_Unit_Of_Trade]);
        sec.lot_size = (uint8_t)stoi(row[MCOL_Round_Lot]);
        sec.shares_outstanding_m = stoi(row[MCOL_Shares_Outstanding]);

        sec.exch_mask.set(Exch_A, row[MCOL_TradedOnNYSEMKT][0]);
        sec.exch_mask.set(Exch_B, row[MCOL_TradedOnNASDAQBX][0]);
        sec.exch_mask.set(Exch_C, row[MCOL_TradedOnNSX][0]);
        sec.exch_mask.set(Exch_D, row[MCOL_TradedOnFINRA][0]);
        sec.exch_mask.set(Exch_I, row[MCOL_TradedOnISE][0]);
        sec.exch_mask.set(Exch_J, row[MCOL_TradedOnEdgeA][0]);
        sec.exch_mask.set(Exch_K, row[MCOL_TradedOnEdgeX][0]);
        sec.exch_mask.set(Exch_M, row[MCOL_TradedOnCHX][0]);
        sec.exch_mask.set(Exch_N, row[MCOL_TradedOnNYSE][0]);
        sec.exch_mask.set(Exch_P, row[MCOL_TradedOnArca][0]);
        sec.exch_mask.set(Exch_V, row[MCOL_TradedOnIEX][0]);
        sec.exch_mask.set(Exch_W, row[MCOL_TradedOnCBOE][0]);
        sec.exch_mask.set(Exch_X, row[MCOL_TradedOnPSX][0]);
        sec.exch_mask.set(Exch_Y, row[MCOL_TradedOnBATSY][0]);
        sec.exch_mask.set(Exch_Z, row[MCOL_TradedOnBATS][0]);
        if (sec.tape == 'C') {
          sec.exch_mask.set(Exch_Q, row[MCOL_TradedOnNasdaq][0]);
        } else {
          sec.exch_mask.set(Exch_T, row[MCOL_TradedOnNasdaq][0]);
        }
        if (MCOL_TradedOnLTSE < (int)row.size()) {
          sec.exch_mask.set(Exch_L, row[MCOL_TradedOnLTSE][0]);
        }
        sec.eff_date = MkTaqDate(row[MCOL_Effective_Date]);
        ctx.output.write((const char *)&sec, sizeof(sec));
        cout << line << endl;
    }
    return 0;
}

}