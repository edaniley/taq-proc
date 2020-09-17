#ifndef TAQ_PREP_TRADES_INCLUDED
#define TAQ_PREP_TRADES_INCLUDED

#include "taq-prep.h"
using namespace std;
using namespace Taq;
using SaleCondintionMap = map<char, pair<char, char>>;

namespace taq_prep {

  class TradeTick {
  public:
    TradeTick(vector<string>& row) :
      symbol(row[TCOL_Symbol]),
      time(MkTaqTime(row[TCOL_Time])),
      exch(row[TCOL_Exchange][0]),
      price(stod(row[TCOL_Trade_Price])),
      qty(stoi(row[TCOL_Trade_Volume])),
      sale_cond(row[TCOL_Sale_Condition]),
      TRF(row[TCOL_Trade_Reporting_Facility][0]),
      correction_ind(stoi(row[TCOL_Trade_Correction_Indicator])),
      source(row[TCOL_Source_of_Trade][0]) {}
    bool IsRegular()        const { return string::npos == sale_cond.find_first_of("479BHMQTUVWZ"); }
    bool IsAuctionPrint()   const { return string::npos == sale_cond.find_first_of("O56"); }
    bool InPrimarySession() const { return string::npos == sale_cond.find('T'); }
    bool IsOpenPrint()      const { return string::npos != sale_cond.find('O'); }
    bool IsOfficialOpen()   const { return string::npos != sale_cond.find('Q'); }
    bool IsClosePrint()     const { return string::npos != sale_cond.find('6'); }
    bool IsOfficialClose()  const { return string::npos != sale_cond.find('M'); }
    bool OnExchange()       const { return exch != 'D'; }
    bool OffExchange()      const { return !OnExchange(); }
    bool IsBlockTrade()     const { return qty >= 10000 || ((price * qty) > 200000.0); }

    const string& symbol;
    const Time time;
    const char exch;
    const double price;
    const int qty;
    const string& sale_cond;
    const char TRF;
    const int correction_ind;
    const char source;
  };

  class SymbolTradeState {
  public:
    SymbolTradeState(const string& symbol, char primary_exch, const SaleCondintionMap& cond_map)
      : symbol(symbol), cond_map(cond_map), primary_exch(primary_exch), last_lte_exch('\0'),
      open_price_primary(.0), open_volume_primary(0), close_price_primary(.0), close_volume_primary(0),
      min_price_regular(std::numeric_limits<double>::max()), max_price_regular(.0),
      trade_count_regular(0), notional_regular(.0),
      volume_total(0), volume_regular(0), volume_trf(0), volume_block(0), volume_pre_open(0), volume_post_close(0),
      volume_regular_continous(0), trade_count_regular_continous(0) {}

    const string symbol;
    const SaleCondintionMap& cond_map;
    const char primary_exch;
    set<char> lte_set;
    Trade::Attr current_trade_attr;
    char      last_lte_exch;

    Taq::Time open_time_primary;
    double    open_price_primary;
    uint64_t  open_volume_primary;
    Taq::Time close_time_primary;
    double    close_price_primary;
    uint64_t  close_volume_primary;
    double    min_price_regular;
    double    max_price_regular;
    int       trade_count_regular;
    double    notional_regular;

    uint64_t  volume_total;           // daily volume
    uint64_t  volume_regular;         // VWAP-eligible i.e. regular prints during main session, including auctions
    uint64_t  volume_trf;             // off-exchange volume
    uint64_t  volume_block;
    uint64_t  volume_pre_open;        // early morning session
    uint64_t  volume_post_close;      // late evening session

    uint64_t  volume_regular_continous;         // volume_regular without auctions i.e. ex opening, intra-day and closing auctions
    int       trade_count_regular_continous;    // trade count - to calculate average execution size

    void SetTradeAttributes(const TradeTick& tick);
    void AccumulateStatistics(const TradeTick& tick);
  };

}

#endif

