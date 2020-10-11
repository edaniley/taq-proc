#ifndef TAQ_PY_INCLUDED
#define TAQ_PY_INCLUDED

#ifdef __unix__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#ifdef __unix__
#pragma GCC diagnostic pop
#endif
#include <iostream>
#include <sstream>
#include <string_view>
#include <charconv>
#include <cstring>
#include <algorithm>
#include <vector>
#include <map>
#include <tuple>
#include <exception>
#include <limits>
#include <typeinfo>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio.hpp>
#ifdef __unix__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
#include <boost/iostreams/stream.hpp>
#ifdef __unix__
#pragma GCC diagnostic pop
#endif
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <string.h>
#ifdef __unix__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <boost/algorithm/string.hpp>
#ifdef __unix__
#pragma GCC diagnostic pop
#endif
#include "taq-text.h"

using namespace std;
using namespace Taq;
using namespace boost::asio;

using Json = boost::property_tree::ptree;
namespace py = pybind11;

using str6 = char[6];
using str12 = char[12]; // Date 1970-01-01 1970-Jan-01
using str18 = char[18]; // Symbol
using str20 = char[20]; // Time 12:30:00.123456789
using str36 = char[36]; // Timestamp 1970-01-01T12:30:00.123456789+00:00
using str64 = char[64]; // Order ID
using str96 = char[96]; // Markouts

struct FieldsDef {
  FieldsDef(const string& name, const string& ctype, size_t size, bool required = true)
    : name(name), ctype(ctype), size(size), required(required), dtype(type(ctype, size))  {}
  const string name;
  const string ctype;
  const size_t size;
  const bool required;
  const string dtype;
  static string type(const string& ctype, size_t size) {
    ostringstream ss;
    if (ctype == typeid(char).name()) {
      ss << 'a' << size;
    } else if (ctype == typeid(double).name()) {
      ss << "double";
    } else if (ctype == typeid(int).name()) {
      ss << "int";
    }
    return ss.str();
  }
};

struct FunctionDef {
  FunctionDef(const string &default_tz, const vector<FieldsDef>& input_fields, const vector<FieldsDef>& output_fields)
    : default_tz(default_tz), input_fields(input_fields), output_fields(output_fields) {}
  const string default_tz;
  const vector<FieldsDef> input_fields;
  const vector<FieldsDef> output_fields;
};

template <typename T>
vector<T> AsVector(Json const& pt, Json::key_type const& key) {
  vector<T> retval;
  try {
    for (auto& item : pt.get_child(key))
      retval.push_back(item.second.get_value<T>());
  } catch (const exception& ex) {
    throw domain_error(string("Inbound json : ") + ex.what());
  }
  return retval;
}

using FunctionFieldsDef = map<string, const FieldsDef&>;
const vector<FieldsDef>& InputFields(const string& function_name);
string JsonToString(const Json&);
Json StringToJson(const string &);

py::list ExecuteImlp(const Json& request, ip::tcp::iostream& tcptream, const py::kwargs& kwargs);

#endif
