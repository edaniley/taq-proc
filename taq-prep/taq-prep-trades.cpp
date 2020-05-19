

#include <algorithm>
#include <boost/algorithm/string.hpp>

#include "taq-prep.h"

using namespace std;

namespace taq_prep {

int ProcessTrades(AppContext &, istream & is) {
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