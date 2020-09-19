

#include <algorithm>
#include <set>
#include <map>
#include <string>
#include <sstream>
#include "boost-algorithm-string.h"

#include "taq-prep-trades.h"

namespace taq_prep {

const SaleCondintionMap CTA_scond_map = {
    // sale condition, sets last trade price, contributes to daily volume
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
};
const SaleCondintionMap UTP_scond_map =  {
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
};

static void ShowProgress() {
  static int rec_cnt = 0;
  static int dot_cnt = 0;
  if (++rec_cnt % 100000 == 0) {
    cout << '.';
    if (++dot_cnt % 100 == 0)
      cout << endl;
  }
}

static bool ValidateInputRecord(const vector<string>& row) {
  if (row[0] == "Time" || row[0] == "END" || row[0].size() == 0)
    return false;
  ShowProgress();
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
  const string& source = row[TCOL_Source_of_Trade];
  if (source.size() != 1 || !strchr("CN", source[0])) {
    throw(domain_error("Source_of_Trade : unexpected value " + source));
  }
  const string& trf = row[TCOL_Trade_Reporting_Facility];
  if (false == (trf.size() == 0 || (trf.size() == 1 && strchr("BNTQ ", trf[0])))) {
    throw(domain_error("Trade_Reporting_Facility : unexpected value " + trf));
  }
  return true;
}

void SymbolTradeState::SetTradeAttributes(const TradeTick &tick) {
  const bool no_correction = tick.correction_ind < 2;
  string sale_cond(tick.sale_cond);
  sale_cond.erase(remove(sale_cond.begin(), sale_cond.end(), ' '), sale_cond.end());
  // determine if trade sets last trade price (is last trade eligible)
  bool is_lte = no_correction;
  for (auto c = sale_cond.begin(); is_lte && c != sale_cond.end(); ++c) {
    auto it = cond_map.find(*c);
    char lte = it != cond_map.end() ? it->second.first : '\0';
    if (lte == 'N') {
      is_lte = false;
    }
    else if (lte == '1') {
      if (lte_set.find(tick.exch) != lte_set.end()) {
        is_lte = false;
      }
    }
    else if (lte == '2' || lte == '4') {
      if (lte_set.size()) {
        is_lte = false;
      }
    }
    else if (lte == '3') {
      if ((lte_set.size() && tick.exch != last_lte_exch) || tick.source == primary_exch) {
        is_lte = false;
      }
    }
    else if (lte == '5') {
      static const Time time_160130 = MkTime(string("16:01:30"));
      is_lte = tick.time < time_160130;
    }
  }
  // update tracking variables if last trade eligible
  if (is_lte) {
    lte_set.insert(tick.exch);
    last_lte_exch = tick.exch;
  }
  // determine if trade contributes to daily total volume (is volume eligible)
  bool is_ve = no_correction;
  for (auto c = sale_cond.begin(); is_ve && c != sale_cond.end(); ++c) {
    auto it = cond_map.find(*c);
    char ve = it != cond_map.end() ? it->second.second : '\0';
    is_ve &= ve != 'N';
  }
  current_trade_attr.lte = is_lte;
  current_trade_attr.ve = is_ve;
  // set remaining attributes
  current_trade_attr.vwap_eligible = is_ve && tick.IsRegular();
  current_trade_attr.primary_session = tick.InPrimarySession();
  current_trade_attr.block_trade = tick.IsBlockTrade();
  current_trade_attr.exch = tick.exch;
  current_trade_attr.trf = tick.TRF;
}

void SymbolTradeState::AccumulateStatistics(const TradeTick& tick) {
  if (primary_exch == tick.exch) {
    if (tick.IsOpenPrint() || (open_volume_primary == 0 && tick.IsOfficialOpen())) {
      open_time_primary = tick.time;
      open_price_primary = tick.price;
      open_volume_primary = tick.qty;
    }
    if (tick.IsClosePrint() || (close_volume_primary == 0 && tick.IsOfficialClose())) {
      close_time_primary = tick.time;
      close_price_primary = tick.price;
      close_volume_primary = tick.qty;
    }
  }
  if (current_trade_attr.ve) {
    volume_total += tick.qty;
    if (current_trade_attr.vwap_eligible) {
      min_price_regular = min(min_price_regular, tick.price);
      max_price_regular = max(max_price_regular, tick.price);
      trade_count_regular += 1;
      notional_regular += tick.price * tick.qty;
      volume_regular += tick.qty;
      if (tick.OffExchange()) {
        volume_trf += tick.qty;
      }
      if (current_trade_attr.block_trade) {
        volume_block += tick.qty;
      }
    }
    if (current_trade_attr.primary_session == false) {
      static const Time time_160000 = MkTime(string("16:00:00"));
      if (tick.time < time_160000) {
        volume_pre_open += tick.qty;
      } else {
        volume_post_close += tick.qty;
      }
    }
    if (tick.IsAuctionPrint() == false) {
      trade_count_regular_continous++;
      volume_regular_continous += tick.qty;
    }
  }
}

static void UpdateSecMasterRecord(Security & security, const SymbolTradeState trd_state) {
  security.volume_total = trd_state.volume_total;
  security.volume_regular = trd_state.volume_regular;
  security.volume_trf = trd_state.volume_trf;
  security.volume_block = trd_state.volume_block;
  security.volume_pre_open = trd_state.volume_pre_open;
  security.volume_post_close = trd_state.volume_post_close;
  security.open_time_primary = trd_state.open_time_primary;
  security.open_price_primary = trd_state.open_price_primary;
  security.open_volume_primary = trd_state.open_volume_primary;
  security.close_time_primary = trd_state.close_time_primary;
  security.close_price_primary = trd_state.close_price_primary;
  security.close_volume_primary = trd_state.close_volume_primary;
  security.average_trade_size = trd_state.trade_count_regular_continous
    ? (int)(((double)trd_state.volume_regular_continous) / trd_state.trade_count_regular_continous +.5) : 0;
  security.min_price_regular = trd_state.min_price_regular;
  security.max_price_regular = trd_state.max_price_regular;
  security.vwap_regular = trd_state.volume_regular ? trd_state.notional_regular / trd_state.volume_regular : .0;
}

void UpdateSecMaster(map<string, unique_ptr<SymbolTradeState>> &symbols) {
  for (const auto& it : symbols) {
    const SymbolTradeState& trd_state = *it.second;
    if (Security* security = GetSecurity(trd_state.symbol)) {
      UpdateSecMasterRecord(*security, trd_state);
    }
  }
  SecMasterFlush();
}

int ProcessTrades(AppContext &ctx, istream & is) {
  vector<SymbolMap> symbol_map;
  map<string, unique_ptr<SymbolTradeState>> symbols;
  const Security* security = nullptr;
  SymbolMap* map_entry = nullptr;
  SymbolTradeState* trd_state = nullptr;
  int rec_cnt = 0;

  SecMasterLoad(ctx);

  ctx.output_file_hdr.type = RecordType::Trade;
  ctx.output.write((const char*)&ctx.output_file_hdr, sizeof(ctx.output_file_hdr));

  while(!is.eof()) {
    string line;
    getline(is, line);
    vector<string> row;
    boost::split(row, line, boost::is_any_of("|"));

    if (ValidateInputRecord(row)) {
      TradeTick tick(row);
      if (!security || security->symb != tick.symbol) { // new symbol
        map_entry = nullptr;
        trd_state = nullptr;
        security = GetSecurity(tick.symbol);
        if (security) {
          symbol_map.push_back(SymbolMap(tick.symbol, rec_cnt + 1, 0));
          map_entry = &*symbol_map.rbegin();
          const SaleCondintionMap& scond_map = tick.source == 'C' ? CTA_scond_map : UTP_scond_map;
          auto it = symbols.emplace(make_pair(tick.symbol, make_unique<SymbolTradeState>(tick.symbol, security->exch, scond_map)));
          trd_state = it.first->second.get();
        }
      }
      if (trd_state) {
        rec_cnt ++;
        trd_state->SetTradeAttributes(tick);
        trd_state->AccumulateStatistics(tick);
        Trade trade(tick.time, tick.price, tick.qty, trd_state->current_trade_attr, tick.sale_cond.c_str());
        ctx.output.write((const char*)&trade, sizeof(trade));
        map_entry->end = rec_cnt;
      }
    }
  }
  // append symbol map
  for (const auto& sm : symbol_map) {
    ctx.output.write((const char*)&sm, sizeof(sm));
  }
  // update file header
  ctx.output_file_hdr.symb_cnt = (int)symbol_map.size();
  ctx.output_file_hdr.rec_cnt = rec_cnt;

  UpdateSecMaster(symbols);
  return 0;
}

}