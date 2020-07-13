

#include <algorithm>
#include "boost-algorithm-string.h"

#include "taq-prep.h"

using namespace std;
using namespace Taq;

namespace taq_prep {

 static void StringCopy(char* desc, const char* src, size_t len) {
#ifdef _MSC_VER
  strncpy_s(desc, len, src, len - 1);
#else
  strncpy(desc, src, len - 1);
#endif
  }

int ProcessSecMaster(AppContext & ctx, istream & is) {
  int cnt = 0;
  cnt++;
  vector<Security> sec_list;
  while(!is.eof()) {
    string line;
    getline(is, line);
    vector<string> row;
    boost::split(row, line, boost::is_any_of("|"));
    if (row[MCOL_Symbol] == "Symbol")
      continue;
    if (row[0] == "END")
      break;
    sec_list.push_back(Security());
    Security & sec =*sec_list.rbegin();
    try {
      memset((void *)&sec, 0, sizeof(sec));

      StringCopy(sec.symb, row[MCOL_Symbol].c_str(), sizeof(sec.symb));
      StringCopy(sec.CUSIP, row[MCOL_CUSIP].c_str(), sizeof(sec.CUSIP));
      StringCopy(sec.type, row[MCOL_Security_Type].c_str(), sizeof(sec.type));
      StringCopy(sec.sip_symb, row[MCOL_SIP_Symbol].c_str(), sizeof(sec.sip_symb));
      StringCopy(sec.prev_symb, row[MCOL_Old_Symbol].c_str(), sizeof(sec.prev_symb));
      StringCopy(sec.industry_code, row[MCOL_NYSE_Industry_Code].c_str(), sizeof(sec.industry_code));

      sec.test_flag = row[MCOL_Test_Symbol_Flag][0];
      sec.exch = row[MCOL_Listed_Exchange][0];
      sec.tape = row[MCOL_Tape][0];
      sec.halt_reason = row[MCOL_Halt_Delay_Reason][0];
      sec.trd_unit = (uint8_t)stoi(row[MCOL_Unit_Of_Trade]);
      sec.lot_size = (uint8_t)stoi(row[MCOL_Round_Lot]);
      sec.shares_outstanding_m = row[MCOL_Shares_Outstanding].empty() ? .0 : stod(row[MCOL_Shares_Outstanding]);

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
      if (false == row[MCOL_Effective_Date].empty()) {
        sec.eff_date = MkTaqDate(row[MCOL_Effective_Date]);
      }
    } catch (exception &ex) {
      cerr << line << endl;
      cerr << "record-cnt:" << sec_list.size() << " err-text" << ex.what() << endl;
    }
  }
  ctx.output_file_hdr.symb_cnt = (int)sec_list.size();
  ctx.output_file_hdr.rec_cnt = (int)sec_list.size();
  ctx.output_file_hdr.type = RecordType::SecMaster;
  ctx.output.write((const char*)&ctx.output_file_hdr, sizeof(ctx.output_file_hdr));
  for (const Security& sec_out : sec_list) {
    ctx.output.write((const char*)&sec_out, sizeof(sec_out));
  }
  return 0;
}

}