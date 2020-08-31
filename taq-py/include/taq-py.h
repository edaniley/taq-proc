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
#include <algorithm>
#include <vector>
#include <map>
#include <exception>
#include <limits>
#include <typeinfo>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio.hpp>
#include <boost/iostreams/stream.hpp>
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

using namespace std;
using namespace boost::asio;
using namespace boost::property_tree;
namespace py = pybind11;

using str6 = char[6];
using str12 = char[12]; // Date 1970-01-01 1970-Jan-01
using str18 = char[18]; // Symbol
using str20 = char[20]; // Time 12:30:00.123456789
using str36 = char[36]; // Timestamp 1970-01-01T12:30:00.123456789+00:00
using str64 = char[64]; // Order ID

struct FieldsDef {
  FieldsDef(const string& name, const string& ctype, size_t size)
    : name(name), ctype(ctype), size(size), dtype(type(ctype, size))  {}
  const string name;
  const string ctype;
  const size_t size;
  const string dtype;
  static string type(const string& ctype, size_t size) {
    ostringstream ss;
    if (ctype == typeid(char).name()) {
      ss << 'a' << size;
    } else if (ctype == typeid(double).name()) {
      ss << "float";
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
vector<T> AsVector(ptree const& pt, ptree::key_type const& key) {
  vector<T> retval;
  try {
    for (auto& item : pt.get_child(key))
      retval.push_back(item.second.get_value<T>());
  } catch (const exception& ex) {
    throw domain_error(string("Inbound json : ") + ex.what());
  }
  return retval;
}

const vector<FieldsDef>& InputFields(const string& function_name);
string JsonToString(const ptree &);
ptree StringToJson(const string &);

py::list ExecuteROD(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs);
py::list ExecuteQuote(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs);
py::list ExecuteVWAP(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs);

inline void StringCopy(char* desc, const char* src, size_t len) {
#ifdef _MSC_VER
  strncpy_s(desc, len, src, len - 1);
#else
  strncpy(desc, src, len - 1);
#endif
}

#endif
