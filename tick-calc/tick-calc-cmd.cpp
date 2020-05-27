
#ifdef _MSC_VER
#pragma comment(lib, "jsoncpp.lib")
#endif
#include <iostream>
#include <json/json.h>

#include "tick-calc.h"
#include "tick-calc-secmaster.h"

using namespace std;
using namespace Taq;

namespace tick_calc {

void HanldeCmd(AppContext & ctx, const string_view& line) {
  cout << line;
  JSONCPP_STRING err;
  Json::Value root;
  Json::CharReaderBuilder builder;
  const unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (reader->parse(line.data(), line.data() + line.size(), &root, &err)) {
    const string cmd = root["cmd"].asString();
    const string date = root["date"].asString();
    const string symbol = root["symbol"].asString();
    cout << "cmd:" << cmd << " date:" << date << " symbol:" << symbol << endl;
    try {
      if (cmd == "load" ) {
        ctx.nbbo_data_manager->LoadSymbolRecordset(MkTaqDate(date), symbol);
      }
      else if (cmd == "unload") {
        ctx.nbbo_data_manager->UnloadSymbolRecordset(MkTaqDate(date), symbol);
      }
    } catch (exception & ex) {
      cerr << ex.what() << endl;
    }
  } else {
    cerr << err << endl;
  }
}

}
