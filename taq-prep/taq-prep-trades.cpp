

#include <algorithm>

#include "taq-prep.h"

using namespace std;

namespace taq_prep {

int ProcessTrades(AppContext &, istream & is) {
      while(!is.eof()) {
        string line;
        getline(is, line);
        cout << line << endl;
    }
    return 0;
}

}