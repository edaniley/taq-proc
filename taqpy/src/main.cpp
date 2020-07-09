#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <map>
#include <exception>
#include <limits>
#include <typeinfo>
#include <json/json.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio.hpp>
#include <boost/iostreams/stream.hpp>
#include <string.h>
#ifdef __unix__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#ifdef __unix__
#pragma GCC diagnostic pop
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <boost/algorithm/string.hpp>
#ifdef __unix__
#pragma GCC diagnostic pop
#endif

#ifdef _MSC_VER
#pragma comment(lib, "jsoncpp.lib")
#endif

using namespace std;
using namespace boost::asio;
namespace py = pybind11;

using str6 = char[6];
using str12 = char[12];
using str18 = char[18];
using str20 = char[20];
using str64 = char[64];

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
  {"Quote", { FieldsDef("Date", typeid(char).name(), 12),
              FieldsDef("Symbol", typeid(char).name(), 18),
              FieldsDef("Time", typeid(char).name(), 18)
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

static string JsonToString(const Json::Value& root) {
  Json::StreamWriterBuilder builder;
  string output = Json::writeString(builder, root);
  output.erase(remove_if(output.begin(), output.end(), [](char c) {return c == '\n'; }), output.end());
  replace(output.begin(), output.end(), '\t', ' ');
  output.append("\n");
  return output;
}

static Json::Value StringToJson(const string& json_str) {
  Json::Value root;
  JSONCPP_STRING err;
  Json::CharReaderBuilder builder;
  const unique_ptr<Json::CharReader> reader(builder.newCharReader());
  if (!reader->parse(json_str.c_str(), json_str.c_str() + json_str.size(), &root, &err)) {
    throw domain_error("Inbound json parsing failure:" + err);
  }
  return root;
}


string MakeReplyHeader(const string& request_id, const string& message) {
  Json::Value error_summary;
  Json::Value error;
  error["type"] = "Client";
  error["message"] = message;
  error_summary.append(error);

  Json::Value root;
  root["request_id"] = request_id;
  root["error_summary"] = error_summary;

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

Json::Value ValidateRequest(const py::args& args, const py::kwargs& kwargs) {
  if (args.size() < 1) {
    throw domain_error("Missing request json");
  }
  JSONCPP_STRING json_err;
  Json::Value   req_json;
  Json::CharReaderBuilder builder;
  const unique_ptr<Json::CharReader> reader(builder.newCharReader());

  string header(args[0].cast< py::str>());
  if (false == reader->parse(header.c_str(), header.c_str() + header.size(), &req_json, &json_err)) {
    throw domain_error("Json parsing error:" + json_err);
  }
  else if (!req_json.isMember("request_id")) {
    throw domain_error("Invalid request missing:request_id");
  }
  else if (!req_json.isMember("function_list")) {
    throw domain_error("Invalid request missing:function_list");
  }
  else if (!req_json.isMember("input_cnt")) {
    throw domain_error("Invalid request missing:input_cnt");
  }
  else if (!req_json.isMember("tcp")) {
    throw domain_error("Invalid request missing:tcp");
  }

  const string request_id = req_json["request_id"].asString();
  const auto& function_list = req_json["function_list"];
  string function_name;
  if (function_list.size() == 0) {
    throw domain_error("Missing function name");
  }
  else if (function_list.size() > 1) {
    throw domain_error("Multi-function request is not supported");
  }
  function_name = function_list[0].asString();

  ValidateInputFields(function_name, kwargs);
  return req_json;
}


py::list ExecuteROD(const Json::Value& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const auto& field_definitions = tick_functions["ROD"];
  const string separator = req_json.isMember("separator") ? req_json["separator"].asString() : string("|");
  const ssize_t input_cnt = req_json["input_cnt"].asInt();
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
  Json::Value response = StringToJson(json_str);
  const size_t record_cnt = response["output_records"].asUInt64();
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
    //cout << line_cnt << " " << line << endl;
    values.clear();
    boost::split(values, line, boost::is_any_of("|"));
#ifdef _MSC_VER
    strncpy_s(ord_id.mutable_at(line_cnt), sizeof(str64), values[0].c_str(), sizeof(str64) - 1);
#else
    strncpy(ord_id.mutable_at(line_cnt), values[0].c_str(), sizeof(str64) - 1);
#endif
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
  //cout << json_str << endl;
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

py::list Execute(py::args args, py::kwargs kwargs) {
  string message("Success");
  string request_id = "N/A";
  try {
    Json::Value req_json = ValidateRequest(args, kwargs);
    request_id = req_json["request_id"].asString();

    string addr = req_json["tcp"].asString();
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

    const string& function_name = req_json["function_list"][0].asString();
    if (function_name == "ROD") {
      return ExecuteROD(req_json, tcptream, kwargs);
    }
  }
  catch (const exception& ex) {
    //cout << ex.what() << endl; // to remove
    message.assign(ex.what());
  }
  py::list retval;
  retval.append(MakeReplyHeader(request_id, message));
  return retval;
}

void Hi() {
  cout << "Hi" << endl;
}

PYBIND11_MODULE(taqpy, m) {
  m.doc() = R"pbdoc(poc)pbdoc";
  m.def("Execute", &Execute, "Interface to tick-proc server");
  m.def("Hi", &Hi, "");
#ifdef VERSION_INFO
  m.attr("__version__") = VERSION_INFO;
#else
  m.attr("__version__") = "dev";
#endif
}
