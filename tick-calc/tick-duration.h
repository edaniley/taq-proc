#ifndef TICK_DURATION_INCLUDED
#define TICK_DURATION_INCLUDED

#include <string_view>
#include <vector>
#include "taq-time.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

enum class DurationType {
  Unknown, Time, Pov, Ticks, MaxVal
};

enum class DurationUnit {
  Unkown, Hour, Min, Sec, Msec, Usec, Tick, Pct, Max
};

union DurationEnding {
  struct Pov {
    Pov(uint64_t volume, double participation) : volume(volume), participation(participation) {}
    uint64_t volume;
    double participation;
  };
  DurationEnding() : pov({ 0,.0 }) {}
  DurationEnding(Time end_time) : time(end_time) {}
  DurationEnding(const Pov& pov) : pov(pov) {}
  DurationEnding(int ticks) : ticks(ticks) {}
  DurationEnding(const DurationEnding& copy) { pov = copy.pov; }
  DurationEnding(DurationEnding&& obj) { pov = obj.pov; }
  DurationEnding& operator = (const DurationEnding& obj) { pov = obj.pov; return *this; }
  Time time;
  Pov pov;
  int ticks;
};

struct Duration {
  Duration(Time end_time) : ending(end_time), type(DurationType::Time) {}
  Duration(const DurationEnding::Pov& pov) : ending(pov), type(DurationType::Pov) {}
  Duration(int ticks) : ending(ticks), type(DurationType::Ticks) {}
  Duration (const string_view& str);
  DurationEnding ending;
  DurationType type;
};

DurationUnit StringToUnit(const string_view& str);
Duration MakeDuration(double val, DurationUnit unit);
Duration DecodeDuration(const string_view& str);
vector<Duration> DecodeMarkouts(const string_view& csv);

}

#endif


