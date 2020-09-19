

#include <map>
#include <charconv>

#include "taq-text.h"
#include "tick-duration.h"

namespace tick_calc {

static const map<string, int> markout_unit_map = {
  {"h", (int)DurationUnit::Hour},
  {"hr", (int)DurationUnit::Hour},
  {"m", (int)DurationUnit::Min},
  {"min", (int)DurationUnit::Min},
  {"s", (int)DurationUnit::Sec},
  {"sec", (int)DurationUnit::Sec},
  {"ms", (int)DurationUnit::Msec},
  {"msec", (int)DurationUnit::Msec},
  {"us", (int)DurationUnit::Usec},
  {"usec", (int)DurationUnit::Usec},
  {"t", (int)DurationUnit::Tick},
  {"tick", (int)DurationUnit::Tick},
  {"%", (int)DurationUnit::Pct}
};

DurationUnit StringToUnit(const string_view& str) {
  auto it = markout_unit_map.find(string(str.data(), str.size()));
  return it == markout_unit_map.end() ? DurationUnit::Unkown : (DurationUnit)it->second;
}

Duration::Duration(const string_view & str) {
  double val = .0;
  type = DurationType::Unknown;
  size_t z = str.find_first_not_of(".0123456789-+");
  if (z != string::npos) {
    string_view sval(str.substr(0, z));
    auto [p, ec] = from_chars(sval.data(), sval.data() + sval.size(), val);
    if (0 == (int)ec) {
      DurationUnit unit = StringToUnit(str.substr(z));
      switch (unit) {
      case DurationUnit::Hour:
        ending.time = Time(boost::posix_time::microseconds((uint64_t)(val * 3600 * 1000000 + (val < 0 ? -0.5 : 0.5))));
        type = DurationType::Time;
        break;
      case DurationUnit::Min:
        ending.time = Time(boost::posix_time::microseconds((uint64_t)(val * 60 * 1000000 + (val < 0 ? -0.5 : 0.5))));
        type = DurationType::Time;
        break;
      case DurationUnit::Sec:
        ending.time = Time(boost::posix_time::microseconds((uint64_t)(val * 1000000 + (val < 0 ? -0.5 : 0.5))));
        type = DurationType::Time;
        break;
      case DurationUnit::Msec:
        ending.time = Time(boost::posix_time::microseconds((uint64_t)(val * 1000 + (val < 0 ? -0.5 : 0.5))));
        type = DurationType::Time;
        break;
      case DurationUnit::Usec:
        ending.time = Time(boost::posix_time::microseconds((uint64_t)(val + (val < 0 ? -0.5 : 0.5))));
        type = DurationType::Time;
        break;
      case DurationUnit::Tick:
        ending.ticks = (int)(val + (val < 0 ? -0.5 : 0.5));
        type = DurationType::Ticks;
        break;
      default:
        break;
      }
    }
  }
  if (type == DurationType::Unknown) {
    throw Exception(ErrorType::InvalidArgument);
  }
}

vector<Duration> DecodeMarkouts(const string_view& csv) {
  vector<Duration> retval;
  vector<string_view> markouts;
  Split(markouts, csv, ',');
  for (const auto& str : markouts) {
    retval.emplace_back(Duration(str));
  }
  return  retval;
}


}
