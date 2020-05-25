

#include <vector>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <iterator>
#include <numeric>
#include <limits>
#include <sstream>
#include <boost/algorithm/string.hpp>

#include "taq-prep.h"
#include "taq-time.h"

using namespace std;
using namespace Taq;

namespace taq_prep {

struct BboSide {
  double price;
  int size;
  bool is_set;
  BboSide() : price(.0), size(0), is_set(false) { }
  BboSide(const string &price, const string &size) : price(stod(price)), size(stoi(size)), is_set(false) { }
};

struct Bbo {
  BboSide bid;
  BboSide offer;
  Bbo() = default;
  Bbo(const vector<string> & row) :
    bid(row[QCOL_Bid_Price], row[QCOL_Bid_Size]),
    offer(row[QCOL_Offer_Price], row[QCOL_Offer_Size]) { }
};

struct NbboSide{
  enum Side {BID, OFFER};
  double price;
  int size;
  ExchangeMask exch_mask;
  NbboSide(Side side) : price( side == BID ? 0 : numeric_limits<double>::max()), size(0) { }
};


struct Nbbo {
  NbboSide bid;
  NbboSide offer;
  Nbbo() : bid(NbboSide::BID), offer(NbboSide::OFFER) {}
};

struct NbboTableEntry {
  Nbbo current_nbbo;
  BboSide exchange_bids[Exch_Max];
  BboSide exchange_offers[Exch_Max];
  //vector<Taq::Nbbo> out_nbbo;
  NbboTableEntry() = default;
};

typedef map<string, NbboTableEntry> NbboTable;

static NbboTable nbbo;

/* ===================================================== page ========================================================*/
static void ValidateQuote(const vector<string> & row, Bbo & bbo) {
    const char src = row[QCOL_Source_Of_Quote][0];
    const char cond = row[QCOL_Quote_Condition][0];
    const char nbbo_ind = row[QCOL_National_BBO_Ind][0];
    if (src == 'C') {
      static string invalid_cta_condition_codes("CLNU4");
      const bool valid = invalid_cta_condition_codes.find(cond) == string::npos;
      bbo.bid.is_set = valid && cond != 'E' && bbo.bid.size > 0;
      bbo.offer.is_set = valid && cond != 'F' && bbo.offer.size > 0;
    } else if (src == 'N') {
      static string invalid_utp_condition_codes("FILNUXZ4");
      const bool valid = invalid_utp_condition_codes.find(cond) == string::npos;
      bbo.bid.is_set = valid && bbo.bid.size > 0 ;
      bbo.offer.is_set = valid && bbo.offer.size > 0;
    } else {
      throw(domain_error("Unknown i source" + src));
    }
}
/* ===================================================== page ========================================================*/
static void ResetNbboSide(NbboTableEntry & entry, NbboSide::Side side) {
  NbboSide & best_quote = side == NbboSide::BID ? entry.current_nbbo.bid : entry.current_nbbo.offer;
  const BboSide * exchange_quotes = side == NbboSide::BID  ? entry.exchange_bids : entry.exchange_offers;
  auto is_better = side == NbboSide::BID
    ? [] (double bid, double best_bid) { return bid > best_bid; }
    : [] (double offer, double best_offer) { return offer < best_offer; };

  best_quote.price = side == NbboSide::BID ? 0 : numeric_limits<double>::max();
  best_quote.size = 0;
  best_quote.exch_mask.reset();
  for (int i = 0; i < Exch_Max; i ++ ) {
    const BboSide & quote = exchange_quotes[i];
    if (quote.is_set) {
      if (is_better(quote.price, best_quote.price)) { // replace best quote
        best_quote.exch_mask.reset();
        best_quote.exch_mask.set(i);
        best_quote.price = quote.price;
        best_quote.size = quote.size;
      } else if (quote.price == best_quote.price) { // join best quote
        best_quote.exch_mask.set(i);
        best_quote.size += quote.size;
      }
    }
  }
}
/* ===================================================== page ========================================================*/
static bool UpdateNbboSide(NbboTableEntry & entry, NbboSide::Side side, int exch_idx, const BboSide & new_quote) {
  BboSide & current_quote = side == NbboSide::BID ? entry.exchange_bids[exch_idx] : entry.exchange_offers[exch_idx];
  NbboSide & best_quote = side == NbboSide::BID ? entry.current_nbbo.bid : entry.current_nbbo.offer;
  const double previous_best_price = best_quote.price;
  const int previous_best_size = best_quote.size;
  auto is_better = side == NbboSide::BID
    ? [] (double bid, double best_bid) { return bid > best_bid; }
    : [] (double offer, double best_offer) { return offer < best_offer; };
  if (new_quote.is_set) {
    if (is_better(new_quote.price, best_quote.price)) {               // replace best quote
      best_quote.exch_mask.reset();
      best_quote.exch_mask.set(exch_idx);
      best_quote.price = new_quote.price;
      best_quote.size = new_quote.size;
    } else if (new_quote.price == best_quote.price) {                 // match best quote
      if (best_quote.exch_mask.test(exch_idx)) {                      // was a conributor already - adjust size of best quote
        best_quote.size += (new_quote.size - current_quote.size);
      } else {                                                        // join best quote
        best_quote.exch_mask.set(exch_idx);
        best_quote.size += new_quote.size;
      }
    } else if (best_quote.exch_mask.test(exch_idx)) {                 // was a conributor to best quote
      if (best_quote.exch_mask.count() > 1) {                         // remove from best quote
        best_quote.size -= current_quote.size;
        best_quote.exch_mask.set(exch_idx, 0);
      } else {                                                        // was the sole conributor - reset best quote
        current_quote = new_quote;
        ResetNbboSide(entry, side);
      }
    } else if (best_quote.exch_mask.count() == 0) {
      throw(logic_error("should not be here"));
    }
  } else if (best_quote.exch_mask.test(exch_idx)) {                 // was a conributor to best quote
      if (best_quote.exch_mask.count() > 1) {                       // remove from best quote
        best_quote.size -= current_quote.size;
        best_quote.exch_mask.set(exch_idx, 0);
      } else {                                                      // was the sole conributor - reset best quote
        current_quote = new_quote;
        ResetNbboSide(entry, side);
      }
  }
  current_quote = new_quote;
  if (best_quote.size == 0) {
    best_quote.price = side == NbboSide::BID ? 0 : numeric_limits<double>::max();
  } else if (best_quote.size < 0) {
    throw(logic_error("Negative best quote size"));
  }
  return best_quote.size != previous_best_size ||best_quote.price != previous_best_price;
}
/* ===================================================== page ========================================================*/
static bool UpdateNbbo(const string & timestamp, const string & symbol, const string & exchange, const Bbo & bbo, ofstream & os) {
  const int exch_idx = exchange[0] - 'A';
  NbboTableEntry & entry = nbbo[symbol];
  bool update_nbbo = UpdateNbboSide(entry, NbboSide::BID, exch_idx, bbo.bid);
  update_nbbo |= UpdateNbboSide(entry, NbboSide::OFFER, exch_idx, bbo.offer);
  if (update_nbbo) {
    Taq::Nbbo record(MkTaqTime(timestamp),
      entry.current_nbbo.bid.price, entry.current_nbbo.offer.price,
      entry.current_nbbo.bid.size, entry.current_nbbo.offer.size);
    os.write((const char*)&record, sizeof(record));
    //entry.out_nbbo.emplace_back(Taq::Nbbo(MkTaqTime(timestamp),
    //                                      entry.current_nbbo.bid.price, entry.current_nbbo.offer.price,
    //                                      entry.current_nbbo.bid.size, entry.current_nbbo.offer.size));
  }
   return update_nbbo;
}
/* ===================================================== page ========================================================*/
static bool ValidateInputRecord(const vector<string> & row) {
  if (row[QCOL_Time] == "Time")
    return false;
  if (row.size() != QCOL_Max) {
    ostringstream ss;
    ss << "Input record size mismatch: expected " << QCOL_Max << " columns, received "<< row.size();
    throw(domain_error(ss.str()));
  }
  if (row[QCOL_Time].size() != 15) {
    throw(domain_error("Unrecognized Time format"));
  }
  return true;
}
/* ===================================================== page ========================================================*/
int ProcessQuotes(AppContext & ctx, istream & is) {
  vector<SymbolMap> symbol_map;
  SymbolMap * current = nullptr;
  size_t rec_cnt = 0;
  ctx.output.write((const char*)&ctx.output_file_hdr, sizeof(ctx.output_file_hdr));
  while (false == is.eof()) {
    string line;
    getline(is, line);
    vector<string> row;
    boost::split(row, line, boost::is_any_of("|"));
    if (row[0] == "END")
      break;
    if (ValidateInputRecord(row)) {
      Bbo bbo(row);
      ValidateQuote(row, bbo);
      if (UpdateNbbo(row[QCOL_Time], row[QCOL_Symbol], row[QCOL_Exchange], bbo, ctx.output)) {
        rec_cnt++;
        if (!current || current->symb != row[QCOL_Symbol]) {
          if (current) {
            current->end = rec_cnt - 1;
          }
          symbol_map.push_back(SymbolMap(row[QCOL_Symbol], rec_cnt, 0));
          current = &*symbol_map.rbegin();
        }
        //if (rec_cnt == 1000000) break;
      }
    }
  }
  if (current) {
    current->end = rec_cnt;
  }
  for (const auto & sm : symbol_map) {
    ctx.output.write((const char*)&sm, sizeof(sm));
  }
  ctx.output_file_hdr.symb_cnt = symbol_map.size();
  ctx.output_file_hdr.rec_cnt = rec_cnt;
  ctx.output_file_hdr.type = RecordType::Nbbo;
  return 0;
}

}
