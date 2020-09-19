#ifndef TICK_JSON_INCLUDED
#define TICK_JSON_INCLUDED

#include <string>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

namespace js = boost::property_tree;

template <typename T>
vector<T> AsVector(js::ptree const& pt, js::ptree::key_type const& key) {
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

js::ptree StringToJson(const string& json_str);
string JsonToString(const js::ptree & root);


#endif
