
#include "tick-func-vwap.h"

using namespace std;
using namespace Taq;

namespace tick_calc {
namespace VWAP {

static bool IsEligible(const Trade& trd, char side, Double price) {
  return trd.attr.ve && (
    price.Empty()
    || (side == 'B' && price.GreaterOrEqual(trd.price))
    || (side == 'S' && price.LessOrEqual(trd.price))
    );
}
static bool IsRegular(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && trd.attr.vwap_eligible;
}
static bool IsExchange(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && trd.attr.vwap_eligible && (char)trd.attr.exch != 'D';
}
static bool IsTRF(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && trd.attr.vwap_eligible && (char)trd.attr.exch == 'D';
}
static bool IsBlock(const Trade& trd, char side, Double price) {
  return IsEligible(trd, side, price) && trd.attr.vwap_eligible && trd.attr.block_trade;
}

Calculator::Calculator(const Trades& trades, Time start_time, const Duration& duration, char side, Double limit_price, Flavor flavor)
  : trades(trades), start_time(start_time), duration(duration), side(side), limit_price(limit_price), flavor(flavor) {
  filter = flavor == Flavor::Regular ? IsRegular
    : flavor == Flavor::Exchange ? IsExchange
    : flavor == Flavor::Block ? IsBlock
    : flavor == Flavor::TRF ? IsTRF : IsEligible;
}

void Calculator::Apply(const Trade& trade) {
  result.trade_count++;
  result.trade_volume += trade.qty;
  result.vwap += trade.price * trade.qty;
}

Result Calculator::Calculate(const Trade* it) {
  switch (duration.type) {
  case DurationType::Time:
    CalculateByTime(it);
    break;
  case DurationType::Pov:
    CalculateByPov(it);
    break;
  case DurationType::Ticks:
    CalculateByTick(it);
    break;
  default:
    break;
  }
  if (result.trade_volume) {
    result.vwap /= result.trade_volume;
  }
  return result;
}

void Calculator::CalculateByTime(const Trade* it) {
  const Time end_time = start_time + duration.ending.time;
  if (start_time <= end_time) { // forward
    for (const Trade* trd = it; trd < trades.end() && trd->time <= end_time; ++trd) {
      if (filter(*trd, side, limit_price)) {
        Apply(*trd);
      }
    }
  }
  else if (it < trades.end()) { // reverse
    if (it->time == start_time && filter(*it, side, limit_price)) {
      Apply(*it);
    }
    for (auto trd = it - 1; it >= trades.begin() && trd->time >= end_time; --trd) {
      if (filter(*trd, side, limit_price)) {
        Apply(*trd);
      }
    }
  }
}

void Calculator::CalculateByPov(const Trade* it) {
  int64_t target_vol = (int64_t)ceil((double)duration.ending.pov.volume / duration.ending.pov.participation);
  for (auto trd = it; trd < trades.end() && target_vol > 0; ++trd) {
    if (filter(*trd, side, limit_price)) {
      Apply(*trd);
      target_vol -= trd->qty;
    }
  }
}

void Calculator::CalculateByTick(const Trade* it) {
  int target_cnt = abs(duration.ending.ticks);
  if (duration.ending.ticks >= 0) { // forward
    for (auto trd = it; trd < trades.end() && target_cnt > 0; ++trd) {
      if (filter(*trd, side, limit_price)) {
        Apply(*trd);
        target_cnt--;
      }
    }
  }
  else if (it < trades.end()) { // reverse
    if (it->time == start_time && filter(*it, side, limit_price)) {
      Apply(*it);
      target_cnt--;
    }
    for (auto trd = it - 1; target_cnt > 0 && trd >= trades.begin(); --trd) {
      if (filter(*trd, side, limit_price)) {
        Apply(*trd);
        target_cnt--;
      }
    }
  }
}

}
}