

#include <algorithm>
#include <set>
#include <map>
#include <string>
#include <sstream>
#include "boost-algorithm-string.h"

#include "taq-prep.h"

using namespace std;
using namespace Taq;

namespace taq_prep {

using SaleCondintionMap = map<char, pair<char, char>>;

const SaleCondintionMap scond_by_src[] = {
  { // CTA
    {'C', {'N', 'Y'}},
    {'B', {'N', 'Y'}},
    {'E', {'Y', 'Y'}},
    {'F', {'Y', 'Y'}},
    {'H', {'N', 'Y'}},
    {'I', {'N', 'Y'}},
    {'K', {'Y', 'Y'}},
    {'L', {'3', 'Y'}},
    {'M', {'N', 'N'}},
    {'N', {'N', 'Y'}},
    {'O', {'1', 'Y'}},
    {'P', {'2', 'Y'}},
    {'Q', {'N', 'N'}},
    {'R', {'N', 'Y'}},
    {'T', {'N', 'Y'}},
    {'U', {'N', 'Y'}},
    {'V', {'N', 'Y'}},
    {'X', {'Y', 'Y'}},
    {'Z', {'2', 'Y'}},
    {'4', {'2', 'Y'}},
    {'5', {'Y', 'Y'}},
    {'6', {'Y', 'Y'}},
    {'7', {'N', 'Y'}},
    {'8', {'N', 'N'}},
    {'9', {'Y', 'N'}}
  },
  { // UTP
    {'@', {'Y', 'Y'}},
    {'A', {'Y', 'Y'}},
    {'B', {'Y', 'Y'}},
    {'C', {'N', 'Y'}},
    {'D', {'Y', 'Y'}},
    {'E', {'N', 'N'}},
    {'F', {'Y', 'Y'}},
    {'G', {'4', 'Y'}},
    {'H', {'N', 'Y'}},
    {'I', {'N', 'Y'}},
    {'K', {'Y', 'Y'}},
    {'L', {'5', 'Y'}},
    {'M', {'N', 'N'}},
    {'N', {'N', 'Y'}},
    {'O', {'Y', 'Y'}},
    {'P', {'4', 'Y'}},
    {'Q', {'N', 'N'}},
    {'R', {'N', 'Y'}},
    {'S', {'Y', 'Y'}},
    {'T', {'N', 'Y'}},
    {'U', {'N', 'Y'}},
    {'V', {'N', 'Y'}},
    {'W', {'N', 'Y'}},
    {'X', {'Y', 'Y'}},
    {'Y', {'Y', 'Y'}},
    {'Z', {'4', 'Y'}},
    {'1', {'Y', 'Y'}},
    {'4', {'4', 'Y'}},
    {'5', {'Y', 'Y'}},
    {'6', {'Y', 'Y'}},
    {'7', {'N', 'Y'}},
    {'8', {'N', 'N'}},
    {'9', {'Y', 'N'}}
  }
};

pair<bool,bool> TradeEligibilityIndicators(const SaleCondintionMap & cond_map, const vector<string> & row,
                                            set<char>& lte_set, char & lte_last_exch) {
  const char ex_id = row[TCOL_Exchange][0];
  const bool not_correction = stoi(row[TCOL_Trade_Correction_Indicator]) < 2;
  string cond = row[TCOL_Sale_Condition];
  cond.erase(remove(cond.begin(), cond.end(), ' '), cond.end());
  // determine if trade is last trade eligible
  bool is_lte = not_correction;
  for (auto c = cond.begin(); is_lte && c != cond.end(); ++ c) {
    auto it = cond_map.find(*c);
    char lte = it != cond_map.end() ? it->second.first : '\0';
    if (lte == 'N') {
      is_lte = false;
    } else if (lte == '1') {
      if (lte_set.find(ex_id) != lte_set.end()) {
        is_lte = false;
      }
    } else if (lte == '2' ||  lte == '4') {
      if (lte_set.size()) {
        is_lte = false;
      }
    } else if (lte == '3') {
      const char source_exch = row[TCOL_Source_of_Trade][0];
      const char primary_exch = PrimaryExchange(row[TCOL_Symbol]);
      if ((lte_set.size() &&  ex_id != lte_last_exch) || source_exch == primary_exch) {
        is_lte = false;
      }
    } else if (lte == '5') {
      static const Time time_160130 = MkTime("16:01:30");
      const Time trd_time = MkTaqTime(row[TCOL_Time]);
      is_lte = trd_time < time_160130;
    }
  }
  // update tracking variables if trade is last trade eligible
  if (is_lte) {
    lte_set.insert(ex_id);
    lte_last_exch = ex_id;
  }
  // determine if trade is volume eligible
  bool is_ve = not_correction;
  for (auto c = cond.begin(); is_ve && c != cond.end(); ++c) {
    auto it = cond_map.find(*c);
    char ve = it != cond_map.end() ? it->second.second : '\0';
    is_ve &= ve != 'N';
  }
  return make_pair(is_lte, is_ve);
}

static bool ValidateInputRecord(const vector<string>& row) {
  if (row[0] == "Time" || row[0] == "END" || row[0].size() == 0)
    return false;
  if (row.size() != TCOL_Max) {
    ostringstream ss;
    ss << "Input record size mismatch: expected " << TCOL_Max << " columns, received " << row.size();
    throw(domain_error(ss.str()));
  }
  if (row[TCOL_Time].size() != 15) {
    throw(domain_error("Unrecognized Time format"));
  }
  if (row[TCOL_Sale_Condition].size() != 4) {
    throw(domain_error("Sale_Condition length is not expected 4 characters"));
  }
  const string& exchange = row[TCOL_Exchange];
  if (exchange.size() != 1 || !strchr("ABCDHIJKLMNPSTQUVWXYZ", exchange[0])) {
    throw(domain_error("Exchange : unexpected value " + exchange));
  }
  const string &source = row[TCOL_Source_of_Trade];
  if (source.size() != 1 || ! strchr("CN" , source[0])) {
    throw(domain_error("Source_of_Trade : unexpected value " + source));
  }
  const string& trf = row[TCOL_Trade_Reporting_Facility];
  if (false == (trf.size() == 0 || (trf.size() == 1 && strchr("BNTQ ", trf[0])))) {
    throw(domain_error("Trade_Reporting_Facility : unexpected value " + trf));
  }
  return true;
}

int ProcessTrades(AppContext &ctx, istream & is) {
  vector<SymbolMap> symbol_map;
  ctx.output_file_hdr.type = RecordType::Trade;
  SymbolMap* current = nullptr;
  int rec_cnt = 0;
  LoadSecMaster(ctx);
  ctx.output.write((const char*)&ctx.output_file_hdr, sizeof(ctx.output_file_hdr));

  const SaleCondintionMap* cond_map = nullptr;
  set<char> symb_lte_set;
  char lte_last_exch = '\0';

  while(!is.eof()) {
    string line;
    getline(is, line);
    vector<string> row;
    Trade::Attr attr;
    boost::split(row, line, boost::is_any_of("|"));

    if (ValidateInputRecord(row)) {
      rec_cnt++;
      if (!current || current->symb != row[TCOL_Symbol]) {
        if (current) {
          current->end = rec_cnt - 1;
        }
        symbol_map.push_back(SymbolMap(row[TCOL_Symbol], rec_cnt, 0));
        current = &*symbol_map.rbegin();

        const char src = row[TCOL_Source_of_Trade][0];
        cond_map = src == 'C' ? &scond_by_src[0] : &scond_by_src[1];
        symb_lte_set.clear();
        lte_last_exch = '\0';
      }
      const Time trd_time = MkTaqTime(row[TCOL_Time]);
      const double trd_price = stod(row[TCOL_Trade_Price]);
      const int trd_qty = stoi(row[TCOL_Trade_Volume]);
      const string& trd_cond = row[TCOL_Sale_Condition];
      attr.exch = row[TCOL_Exchange][0];
      attr.trf = row[TCOL_Trade_Reporting_Facility][0];
      auto indicators = TradeEligibilityIndicators(*cond_map, row, symb_lte_set, lte_last_exch);
      attr.lte = indicators.first;
      attr.ve = indicators.second;
      attr.iso = '1' == row[TCOL_Trade_Through_Exempt_Indicator][0] ? 1 : 0;
      Trade trade(trd_time, trd_price, trd_qty, attr, trd_cond.c_str());
      ctx.output.write((const char*)&trade, sizeof(trade));
    }
    if (current) {
      current->end = rec_cnt;
    }
  }
  for (const auto& sm : symbol_map) {
    ctx.output.write((const char*)&sm, sizeof(sm));
  }
  ctx.output_file_hdr.symb_cnt = (int)symbol_map.size();
  ctx.output_file_hdr.rec_cnt = rec_cnt;
  return 0;

}

}