#ifndef TAQ_PREP_INCLUDED
#define TAQ_PREP_INCLUDED

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

namespace taq_prep
{
  struct AppContext {
    std::string date;
    std::string input_type;
    std::vector<std::string> input_files;
    std::string output_dir;
  };

int ProcessSecMaster(AppContext &, std::istream & is);
int ProcessQuotes(AppContext &, std::istream & is);
int ProcessTrades(AppContext &, std::istream & is);

}

#endif
