
#include "taq-py.h"

map<string, FunctionDef> tick_functions = {
  { 
    "Quote",
    FunctionDef(
        "America/New_York", {
          FieldsDef("Symbol", typeid(char).name(), 18),
          FieldsDef("Timestamp", typeid(char).name(), 36)
        }, {
          FieldsDef("ID", typeid(int).name(), sizeof(int)),
          FieldsDef("Timestamp", typeid(char).name(), 36),
          FieldsDef("BestBidPx", typeid(double).name(), sizeof(double)),
          FieldsDef("BestBidQty", typeid(char).name(), sizeof(int)),
          FieldsDef("BestOfferPx", typeid(double).name(), sizeof(double)),
          FieldsDef("BestOfferQty", typeid(char).name(), sizeof(int))
        }
      )
  },
  {
    "ROD",
    FunctionDef(
      "UTC", {
        FieldsDef("ID", typeid(char).name(), 64),
        FieldsDef("Symbol", typeid(char).name(), 18),
        FieldsDef("Date", typeid(char).name(), 12),
        FieldsDef("StartTime", typeid(char).name(), 20),
        FieldsDef("EndTime", typeid(char).name(), 20),
        FieldsDef("Side", typeid(char).name(), 6),
        FieldsDef("OrdQty", typeid(double).name(), sizeof(double)),
        FieldsDef("LimitPx", typeid(double).name(), sizeof(double)),
        FieldsDef("MPA", typeid(double).name(),  sizeof(double)),
        FieldsDef("ExecTime", typeid(char).name(), 20),
        FieldsDef("ExecQty", typeid(double).name(),  sizeof(double))
      }, {
        FieldsDef("Symbol", typeid(char).name(), 64),
        FieldsDef("MinusThree", typeid(double).name(), sizeof(double)),
        FieldsDef("MinusTwo", typeid(double).name(), sizeof(double)),
        FieldsDef("MinusOne", typeid(double).name(), sizeof(double)),
        FieldsDef("Zero", typeid(double).name(), sizeof(double)),
        FieldsDef("PlusOne", typeid(double).name(), sizeof(double)),
        FieldsDef("PlusTwo", typeid(double).name(), sizeof(double)),
        FieldsDef("PlusThree", typeid(double).name(), sizeof(double))
      }
    )
  }
};

