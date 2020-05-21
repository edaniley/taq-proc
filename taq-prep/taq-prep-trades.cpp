

#include <algorithm>
#include <boost/algorithm/string.hpp>

#include "taq-prep.h"

using namespace std;
using namespace Taq;

namespace taq_prep {

int ProcessTrades(AppContext &ctx, istream & is) {
  ctx.output_file_hdr.type = FileHeader::Type::Trade;
  while(!is.eof()) {
    string line;
    getline(is, line);
    vector<string> row;
    boost::split(row, line, boost::is_any_of("|"));

    cout << line << endl;
  }
  return 0;
}

}