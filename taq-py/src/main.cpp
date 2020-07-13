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

static void StringCopy(char *desc, const char *src, size_t len) {
#ifdef _MSC_VER
  strncpy_s(desc, len, src, len - 1);
#else
strncpy(desc, src, len - 1);
#endif
}

struct FieldsDef {
  FieldsDef(const string& name, const string& dtype, size_t size)
    : name(name), dtype(dtype), size(size) {}
  const string name;
  const string dtype;
  const size_t size;
};
struct FunctionDef {
  FunctionDef(const string& name, const vector<FieldsDef>& args) : name(name), args(args) {}
  const string name;
  const vector<FieldsDef> args;
};

map<string, vector<FieldsDef>> tick_functions = {
  {"Quote", { FieldsDef("Symbol", typeid(char).name(), 18),
              FieldsDef("Timestamp", typeid(char).name(), 36)
            }
  },
  {"ROD" , {  FieldsDef("ID", typeid(char).name(), 64),
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
            }
  }
};

template <typename T>
vector<T> AsVector(ptree const& pt, ptree::key_type const& key) {
  vector<T> retval;
  try {
    for (auto& item : pt.get_child(key))
      retval.push_back(item.second.get_value<T>());
  }
  catch (const exception& ex) {
    throw domain_error(string("Inbound json : ") + ex.what());
  }
  return retval;
}

static string JsonToString(const ptree& root) {
  stringstream ss;
  write_json(ss, root);
  string output = ss.str();
  output.erase(remove_if(output.begin(), output.end(), [](char c) {return c == '\n'; }), output.end());
  replace(output.begin(), output.end(), '\t', ' ');
  output.append("\n");
  return output;
}

static ptree StringToJson(const string& json_str) {
  ptree root;
  stringstream ss;
  ss << json_str;
  try {
    read_json(ss, root);
  }
  catch (const exception& ex) {
    throw domain_error(string("Inbound json parsing failure:") + ex.what());
  }
  return root;
}


string MakeReplyHeader(const string& request_id, const string& message) {
  stringstream ss;
  ptree root, error_summary, error;
  error.put("type", "Client");
  error.put("count", 1);
  error_summary.push_front(make_pair("", error));
  root.add_child("error_summary", error_summary);
  root.put("request_id", request_id);
  root.put("error_message", message);
  return JsonToString(root);
}

static void ValidateInputFields(const string& function_name, const py::kwargs& kwargs) {
  const auto& field_definitions = tick_functions[function_name];
  if (kwargs.size() != field_definitions.size()) {
    throw domain_error("Input field count mismatch");
  }
  for (const auto& fdef : field_definitions) {
    if (!kwargs.contains(fdef.name.c_str())) {
      throw domain_error("Missing field name:" + fdef.name);
    }
  }
}

ptree ValidateRequest(const py::args& args, const py::kwargs& kwargs) {
  if (args.size() < 1) {
    throw domain_error("Missing request json");
  }
  string header(args[0].cast< py::str>());
  const ptree req_json = StringToJson(header);
  const string request_id = req_json.get<string>("request_id", "");
  const string tcp = req_json.get<string>("tcp", "");
  const int input_cnt = req_json.get<int>("input_cnt", 0);
  const vector<string> function_list = AsVector<string>(req_json, "function_list");
  if (input_cnt == 0) {
    throw domain_error("Invalid request missing:input_cnt");
  } else if (tcp.size() == 0) {
    throw domain_error("Invalid request missing:tcp");
  } else if (function_list.size() == 0) {
    throw domain_error("Missing function name");
  } else if (function_list.size() > 1) {
    throw domain_error("Multi-function request is not supported");
  }
  const string & function_name = function_list[0];
  ValidateInputFields(function_name, kwargs);
  return req_json;
}

py::list ExecuteROD(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const auto& field_definitions = tick_functions["ROD"];
  const string separator = req_json.get<string>("separator", "|");
  const ssize_t input_cnt = req_json.get<ssize_t>("input_cnt", 0);
  vector<function<void(ostream& os, size_t)>> func;//(field_definitions.size());
  ostringstream ss;

  py::array_t<str64> arr_id = kwargs["ID"].cast<py::array_t<str64>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_id.at(i) << separator; });

  py::array_t<str18> arr_symb = kwargs["Symbol"].cast<py::array_t<str18>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_symb.at(i) << separator; });

  py::array_t<str12> arr_date = kwargs["Date"].cast<py::array_t<str12>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_date.at(i) << separator; });

  py::array_t<str20> arr_stime = kwargs["StartTime"].cast<py::array_t<str20>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_stime.at(i) << separator; });

  py::array_t<str20> arr_etime = kwargs["EndTime"].cast<py::array_t<str20>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_etime.at(i) << separator; });

  py::array_t<str6> arr_side = kwargs["Side"].cast<py::array_t<str6>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_side.at(i) << separator; });

  py::array_t<double> arr_oqty = kwargs["OrdQty"].cast<py::array_t<double>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_oqty.at(i) << separator; });

  py::array_t<double> arr_lpx = kwargs["LimitPx"].cast<py::array_t<double>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_lpx.at(i) << separator; });

  py::array_t<double> arr_mpa = kwargs["MPA"].cast<py::array_t<double>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_mpa.at(i) << separator; });

  py::array_t<str20> arr_xtime = kwargs["ExecTime"].cast<py::array_t<str20>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_xtime.at(i) << separator; });

  py::array_t<double> arr_xqty = kwargs["ExecQty"].cast<py::array_t<double>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_xqty.at(i) << endl; });

  tcptream << JsonToString(req_json);
  for (auto i = 0; i < input_cnt; i++) {
    for_each(func.begin(), func.end(), [&](auto f) {f(ss, i); });
    if (ss.str().size() > 64 * 1024) {
      tcptream << ss.str();
      ss.str("");
      ss.clear();
    }
  }
  tcptream << ss.str();

  string json_str;
  getline(tcptream, json_str);
  ptree response = StringToJson(json_str);
  const size_t record_cnt = response.get<size_t>("output_records", 0);
  py::array_t<str64> ord_id(record_cnt);
  memset(ord_id.mutable_data(), 0, ord_id.nbytes());
  py::array_t<double> minus3((record_cnt));
  py::array_t<double> minus2((record_cnt));
  py::array_t<double> minus1((record_cnt));
  py::array_t<double> zero((record_cnt));
  py::array_t<double> plus1((record_cnt));
  py::array_t<double> plus2((record_cnt));
  py::array_t<double> plus3((record_cnt));

  int line_cnt = 0;
  string line;
  vector<string> values;
  while (getline(tcptream, line)) {
    values.clear();
    boost::split(values, line, boost::is_any_of("|"));
    StringCopy(ord_id.mutable_at(line_cnt), values[0].c_str(), sizeof(str64));
    minus3.mutable_at(line_cnt) = stod(values[1]);
    minus2.mutable_at(line_cnt) = stod(values[2]);
    minus1.mutable_at(line_cnt) = stod(values[3]);
    zero.mutable_at(line_cnt) = stod(values[4]);
    plus1.mutable_at(line_cnt) = stod(values[5]);
    plus2.mutable_at(line_cnt) = stod(values[6]);
    plus3.mutable_at(line_cnt) = stod(values[7]);
    line_cnt++;
  }
  py::list retval;
  retval.append(json_str);
  retval.append(ord_id);
  retval.append(minus3);
  retval.append(minus2);
  retval.append(minus1);
  retval.append(zero);
  retval.append(plus1);
  retval.append(plus2);
  retval.append(plus3);
  tcptream.close();
  return retval;

  // todo
  //for (const auto fdef : field_definitions) {
  //  const char* key = fdef.name.c_str();
  //  const auto& it = kwargs[key];
  //  if (fdef.dtype == typeid(char).name()) {
  //    switch (fdef.size) {
  //    case 20: {
  //    }
  //    case 18: {
  //    }
  //    case 12: {
  //    }
  //    case 6: {
  //    }
  //    case 64: {
  //    }
  //    default:
  //      throw domain_error("Unexpected char type size");
  //    }
  //  } else if (fdef.dtype == typeid(double).name()) {
  //  } else if (fdef.dtype == typeid(int).name()) {
  //  }
  //}
}

py::list ExecuteQuote(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const auto& field_definitions = tick_functions["Quote"];
  const string separator = req_json.get<string>("separator", "|");
  const ssize_t input_cnt = req_json.get<ssize_t>("input_cnt", 0);
  vector<function<void(ostream& os, size_t)>> func;
  ostringstream ss;

  py::array_t<str18> arr_symb = kwargs["Symbol"].cast<py::array_t<str18>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_symb.at(i) << separator; });

  py::array_t<str36> arr_time = kwargs["Timestamp"].cast<py::array_t<str36>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_time.at(i) << endl; });

  tcptream << JsonToString(req_json);
  for (auto i = 0; i < input_cnt; i++) {
    for_each(func.begin(), func.end(), [&](auto f) {f(ss, i); });
    if (ss.str().size() > 64 * 1024) {
      tcptream << ss.str();
      ss.str("");
      ss.clear();
    }
  }
  tcptream << ss.str();

  string json_str;
  getline(tcptream, json_str);
  cout << "RCVD: " << json_str << endl;
  ptree response = StringToJson(json_str);
  const size_t record_cnt = response.get<size_t>("output_records", 0);
  py::array_t<int> id((record_cnt));
  py::array_t<str20> time(record_cnt); // 09:35:28.123456789
  memset(time.mutable_data(), 0, time.nbytes());
  py::array_t<double> bidp((record_cnt));
  py::array_t<int> bids((record_cnt));
  py::array_t<double> askp((record_cnt));
  py::array_t<int> asks((record_cnt));

  int line_cnt = 0;
  string line;
  vector<string> values;
  while (getline(tcptream, line)) {
    values.clear();
    boost::split(values, line, boost::is_any_of("|"));
    id.mutable_at(line_cnt) = stoi(values[0]);
    StringCopy(time.mutable_at(line_cnt), values[1].c_str(), sizeof(str20));
    bidp.mutable_at(line_cnt) = stod(values[2]);
    bids.mutable_at(line_cnt) = stoi(values[3]);
    askp.mutable_at(line_cnt) = stod(values[4]);
    asks.mutable_at(line_cnt) = stoi(values[5]);
    line_cnt++;
  }
  py::list retval;
  retval.append(json_str);
  retval.append(id);
  retval.append(time);
  retval.append(bidp);
  retval.append(bids);
  retval.append(askp);
  retval.append(asks);
  tcptream.close();
  return retval;
}

py::list Execute(py::args args, py::kwargs kwargs) {
  string message("Success");
  string request_id = "N/A";
  try {
    ptree req_json = ValidateRequest(args, kwargs);
    const string request_id = req_json.get<string>("request_id", "");

    const string addr = req_json.get<string>("tcp", "");
    auto z = addr.find(":");
    if (z == string::npos) {
      throw domain_error("Invalid server address:" + addr);
    }
    string ip = string(addr.begin(), addr.begin() + z);
    string tcp = string(addr.begin() + z + 1, addr.end());
    ip::tcp::iostream tcptream;
    tcptream.connect(ip, tcp);
    if (!tcptream) {
      throw domain_error(tcptream.error().message());
    }

    const string function_name = AsVector<string>(req_json, "function_list")[0];
    if (function_name == "ROD") {
      return ExecuteROD(req_json, tcptream, kwargs);
    } if (function_name == "Quote") {
      return ExecuteQuote(req_json, tcptream, kwargs);
    } else {
      throw domain_error("Unknown function:" + function_name);
    }
  }
  catch (const exception& ex) {
    message.assign(ex.what());
  }
  py::list retval;
  retval.append(MakeReplyHeader(request_id, message));
  return retval;
}

void Version() {
  cout << "v2" << endl;
}

PYBIND11_MODULE(taqpy, m) {
  m.doc() = R"pbdoc(poc)pbdoc";
  m.def("Execute", &Execute, "Interface to tick-proc server");
  m.def("Version", &Version, "");
#ifdef VERSION_INFO
  m.attr("__version__") = VERSION_INFO;
#else
  m.attr("__version__") = "dev";
#endif
}
