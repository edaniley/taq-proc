
#include "taq-py.h"

map<string, FunctionDef> tick_functions = {
  { 
    "NBBO",
    FunctionDef(
        "America/New_York", {
          FieldsDef("Symbol", typeid(char).name(), 18),
          FieldsDef("Timestamp", typeid(char).name(), 36),
          FieldsDef("Markouts", typeid(char).name(), 96, false)
        }, {
          FieldsDef("ID", typeid(int).name(), sizeof(int)),
          FieldsDef("Timestamp", typeid(char).name(), 36),
          FieldsDef("BestBidPx", typeid(double).name(), sizeof(double)),
          FieldsDef("BestBidQty", typeid(int).name(), sizeof(int)),
          FieldsDef("BestOfferPx", typeid(double).name(), sizeof(double)),
          FieldsDef("BestOfferQty", typeid(int).name(), sizeof(int))
        }
      )
  },
  {
    "NBBOPrice",
    FunctionDef(
        "America/New_York", {
          FieldsDef("Symbol", typeid(char).name(), 18),
          FieldsDef("Timestamp", typeid(char).name(), 36),
          FieldsDef("Markouts", typeid(char).name(), 96, false)
        }, {
          FieldsDef("ID", typeid(int).name(), sizeof(int)),
          FieldsDef("Timestamp", typeid(char).name(), 36),
          FieldsDef("BestBidPx", typeid(double).name(), sizeof(double)),
          FieldsDef("BestOfferPx", typeid(double).name(), sizeof(double))
        }
      )
  },
  {
    "VWAP",
    FunctionDef(
        "America/New_York", {
          FieldsDef("Symbol", typeid(char).name(), 18),
          FieldsDef("Date", typeid(char).name(), 12),
          FieldsDef("StartTime", typeid(char).name(), 20),
          FieldsDef("Side", typeid(char).name(), 6, false),
          FieldsDef("LimitPx", typeid(double).name(), sizeof(double), false),
          FieldsDef("Flavor", typeid(char).name(), 6, false),
          FieldsDef("EndTime", typeid(char).name(), 20, false),
          FieldsDef("TargetVolume", typeid(int).name(), sizeof(int), false),
          FieldsDef("TargetPOV", typeid(double).name(), sizeof(double), false),
          FieldsDef("Ticks", typeid(int).name(), sizeof(int), false),
          FieldsDef("Markouts", typeid(char).name(), 96, false)
        }, {
          FieldsDef("ID", typeid(int).name(), sizeof(int)),
          FieldsDef("TradeCnt", typeid(int).name(), sizeof(int)),
          FieldsDef("TradeVolume", typeid(int).name(), sizeof(int)),
          FieldsDef("VWAP", typeid(double).name(), sizeof(double)),
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

