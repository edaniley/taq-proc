

#include <vector>
#include <unordered_map>
#include <bitset>
#include <algorithm>
#include <iterator>
#include <numeric>
#include <limits>
#include <sstream>
#include <boost/algorithm/string.hpp>

#include "taq-prep.h"
#include "taq-time.h"

using namespace std;
using namespace taq_proc;
namespace dt = boost::date_time;




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

typedef std::bitset<Exch_Max> ExchangeMask;

struct NbboSide{
  enum Side {BID, OFFER};
  double price;
  int size;
  ExchangeMask exch_mask;
  NbboSide(Side side) : price( side == BID ? 0 : numeric_limits<double>::max()), size(0) { }
  const string toString(Side side) const {
    char char_mask[Exch_Max];
    size_t idx = 0;
    for (size_t i = 0 ; i < exch_mask.size(); i ++) {
      if (exch_mask.test(i)) {
        char_mask[idx ++] = (char) ('A' + i);
      }
    }
    char_mask[idx] = '\0';
    ostringstream ss;
    if ((side == BID && price == 0) || (side == OFFER && price == numeric_limits<double>::max())) {
      ss << "NA";
    } else {
      ss << price;
    }
    ss << ',' << size << ',' << char_mask;
    return ss.str();
  }
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
  NbboTableEntry() = default;
};

typedef unordered_map<string, NbboTableEntry> NbboTable;

static NbboTable nbbo;

static bool ValidateQuote(const vector<string> & row, Bbo & bbo) {
    const char src = row[QCOL_Source_Of_Quote][0];
    const char cond = row[QCOL_Quote_Condition][0];
    const char nbbo_ind = row[QCOL_National_BBO_Ind][0];
    bool nbbo_update = false;
    if (src == 'C') {
      static string invalid_cta_condition_codes("CLNU4");
      const bool valid = invalid_cta_condition_codes.find(cond) == string::npos;
      bbo.bid.is_set = valid && cond != 'E' && bbo.bid.size > 0;
      bbo.offer.is_set = valid && cond != 'F' && bbo.offer.size > 0;
      nbbo_update = nbbo_ind == 'G' || nbbo_ind == 'T' || nbbo_ind == 'U';
    } else if (src == 'N') {
      static string invalid_utp_condition_codes("FILNUXZ4");
      const bool valid = invalid_utp_condition_codes.find(cond) == string::npos;
      bbo.bid.is_set = valid && bbo.bid.size > 0 ;
      bbo.offer.is_set = valid && bbo.offer.size > 0;
      nbbo_update = nbbo_ind == '2' || nbbo_ind == '3' || nbbo_ind == '4';
    } else {
      throw(domain_error("Unknown i source" + src));
    }
    return nbbo_update;
}

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


static bool UpdateNbboSide(NbboTableEntry & nbbo, NbboSide::Side side, int exch_idx, const BboSide & new_quote) {
  BboSide & current_quote = side == NbboSide::BID ? nbbo.exchange_bids[exch_idx] : nbbo.exchange_offers[exch_idx];
  NbboSide & best_quote = side == NbboSide::BID ? nbbo.current_nbbo.bid : nbbo.current_nbbo.offer;
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
        ResetNbboSide(nbbo, side);
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
        ResetNbboSide(nbbo, side);
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

static bool HandleQuote(const string & timestamp, const string & symbol, const string & exchange, const Bbo & bbo) {
  const int exch_idx = exchange[0] - 'A';
  NbboTableEntry & entry = nbbo[symbol];
  bool update_nbbo = UpdateNbboSide(entry, NbboSide::BID, exch_idx, bbo.bid);
  update_nbbo |= UpdateNbboSide(entry, NbboSide::OFFER, exch_idx, bbo.offer);
  if (update_nbbo) {
    cout << boost::posix_time::to_simple_string(MkTaqTime(timestamp))
         << ","  << symbol
         << "," << entry.current_nbbo.bid.toString(NbboSide::BID)
         << "," << entry.current_nbbo.offer.toString(NbboSide::OFFER)
         << endl;
  }
  return update_nbbo;
}

static void PrintTaqRecord(const vector<string> rec) {
  static int rec_no = 0;
  if ((rec_no +1) == 72 || (rec_no +1) == 64) {
    static int here = 0;
    here++;
  }
  cout << "rec_no:" << ++rec_no
       << " time:" << rec[QCOL_Time]
       << " exch:"  << rec[QCOL_Exchange]
       << " src:"  << rec[QCOL_Source_Of_Quote]
       << " symb:"  << rec[QCOL_Symbol]
       << " bid:"  << rec[QCOL_Bid_Size] << "@" << rec[QCOL_Bid_Price]
       << " ofr:"  << rec[QCOL_Offer_Size] << "@" << rec[QCOL_Offer_Price]
       << " nbbo:"  << rec[QCOL_National_BBO_Ind] << endl;
}

static void ValidateSide(const NbboTableEntry & entry, NbboSide::Side side, int idx) {
  const NbboSide & best_quote = side == NbboSide::BID ? entry.current_nbbo.bid : entry.current_nbbo.offer;
  const BboSide & quote = side == NbboSide::BID ? entry.exchange_bids[idx] :  entry.exchange_offers[idx];
  if (quote.is_set) {
    bool invalid = best_quote.exch_mask.test(idx) && quote.price != best_quote.price;
    invalid |= !best_quote.exch_mask.test(idx) && quote.price == best_quote.price;
    if (invalid)
        throw(domain_error("Invalid quote state"));
  }
}

static void PrintSide(const BboSide & quote) {
  if (quote.is_set) {
    string size = to_string(quote.size);
    size.insert(size.begin(), 4 - size.size(), ' ');
    string price = to_string(quote.price);
    auto z = find_if_not(price.rbegin(), price.rend(), [] (char c) {return c == '0';});
    price.erase(z.base(), price.end());
    price.insert(price.begin(), 12 - price.size(), ' ');
    cout << price << ' ' << size;
  } else {
    cout << string(17, ' ');
  }
}

static void PrintQuotes(const string & symbol) {
  const NbboTableEntry & entry = nbbo[symbol];
  const NbboSide & best_bid = entry.current_nbbo.bid;
  const NbboSide & best_offer = entry.current_nbbo.offer;
  for (int i = 0; i < Exch_Max; i ++) {
    const BboSide & bid = entry.exchange_bids[i];
    const BboSide & offer = entry.exchange_offers[i];
    if (bid.is_set || offer.is_set) {
      ValidateSide(entry, NbboSide::BID, i);
      ValidateSide(entry, NbboSide::OFFER, i);
      cout << (char)('A' + i);
      cout << (best_bid.exch_mask.test(i) ? "    >" : "     ");
      PrintSide(bid);
      cout << (best_offer.exch_mask.test(i) ? "    >" : "     ");
      PrintSide(offer);
      cout << endl;
    }
  }
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
    bool expected = ValidateQuote(row, bbo);
    bool updated = HandleQuote(row[QCOL_Time], row[QCOL_Symbol], row[QCOL_Exchange], bbo);
    if (expected ^ updated) {
      //cout << "expected:" << to_string(expected) << " updated:" << to_string(updated) << endl;
    }
  }
  return 0;
}

}
