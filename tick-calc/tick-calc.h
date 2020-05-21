#ifndef TICK_CALC_INCLUDED
#define TICK_CALC_INCLUDED

#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "taq-proc.h"
#include "tick-calc-secmaster.h"

namespace tick_calc
{

struct AppContext {
  std::string data_dir;
};




bool LoadFile(const std::string & data_dir, Taq::FileHeader::Type type, Taq::Date date, char symbol_group);

}
#endif

