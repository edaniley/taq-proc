#ifndef TICK_JSON_INCLUDED
#define TICK_JSON_INCLUDED

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

using namespace std;

using Json = boost::property_tree::ptree;

template <typename T>
vector<T> AsVector(Json const & pt, Json::key_type const& key) {
  vector<T> retval;
  try {
    for (auto& item : pt.get_child(key))
      retval.push_back(item.second.get_value<T>());
  }
  catch (const exception& ex) {
    throw domain_error(string("Json error: ") + ex.what());
  }
  return retval;
}

Json StringToJson(const string & str);
string JsonToString(const Json & json);
string JsonRemoveWhiteChars(const string & str);


#endif
