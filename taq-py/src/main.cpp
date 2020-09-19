#include "taq-py.h"

extern map<string, FunctionDef> tick_functions;

string JsonToString(const ptree& root) {
  stringstream ss;
  write_json(ss, root);
  string output = ss.str();
  output.erase(remove_if(output.begin(), output.end(), [](char c) {return c == '\n'; }), output.end());
  replace(output.begin(), output.end(), '\t', ' ');
  return output;
}

ptree StringToJson(const string& json_str) {
  ptree root;
  stringstream ss;
  ss << json_str;
  try {
    read_json(ss, root);
  }
  catch (const exception & ex) {
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
  const auto& field_definitions = InputFields(function_name);
  for (const auto & fdef : field_definitions) {
    if (fdef.required && false == kwargs.contains(fdef.name.c_str())) {
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

py::list Execute(py::args args, py::kwargs kwargs) {
  string message("Success");
  string request_id = "N/A";
  try {
    ptree req_json = ValidateRequest(args, kwargs);
    const string request_id = req_json.get<string>("request_id", "");

    const string tcpip_addr = req_json.get<string>("tcp", "");
    auto z = tcpip_addr.find(":");
    if (z == string::npos) {
      throw domain_error("Invalid server address:" + tcpip_addr);
    }
    string ip = string(tcpip_addr.begin(), tcpip_addr.begin() + z);
    string tcp = string(tcpip_addr.begin() + z + 1, tcpip_addr.end());
    ip::tcp::iostream tcptream;
    tcptream.connect(ip, tcp);
    if (!tcptream) {
      throw domain_error(tcptream.error().message());
    }

    const string function_name = AsVector<string>(req_json, "function_list")[0];
    if (function_name == "ROD") {
      return ExecuteROD(req_json, tcptream, kwargs);
    } if (function_name == "NBBO") {
      return ExecuteNBBO(req_json, tcptream, kwargs);
    } if (function_name == "NBBOPrice") {
      return ExecuteNBBOPrice(req_json, tcptream, kwargs);
    } if (function_name == "VWAP") {
      return ExecuteVWAP(req_json, tcptream, kwargs);
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

const vector<FieldsDef>& InputFields(const string& function_name) {
  const auto it = tick_functions.find(function_name);
  if (it == tick_functions.end()) {
    domain_error("Unsupported function:" + function_name);
  }
  return it->second.input_fields;
}

string Describe() {
  ptree root;
  for (const auto fit : tick_functions) {
    ptree function, input_fields, output_fields;
    const string &function_name = fit.first;
    const FunctionDef& function_def = fit.second;
    for (const auto &fdef : function_def.input_fields) {
      ptree fld;
      fld.put("name", fdef.name);
      fld.put("dtype", fdef.dtype);
      input_fields.push_back(make_pair("", fld));
    }
    for (const auto& fdef : function_def.output_fields) {
      ptree fld;
      fld.put("name", fdef.name);
      fld.put("dtype", fdef.dtype);
      output_fields.push_back(make_pair("", fld));
    }
    function.put("default_tz", function_def.default_tz);
    function.add_child("input_fields", input_fields);
    function.add_child("output_fields", output_fields);
    root.add_child(function_name, function);
  }
  return JsonToString(root);
}

py::list FunctionList() {
  py::list retval;
  for (const auto fit : tick_functions) {
    retval.append(fit.first);
  }
  return retval;
}

py::list ArgumentList(const string function_name) {
  py::list retval;
  const auto fit = tick_functions.find(function_name);
  if (fit != tick_functions.end()) {
    for (const auto& fdef : fit->second.input_fields) {
      retval.append(make_pair(fdef.name, fdef.dtype));
    }
  }
  return retval;
}

py::list ArgumentNames(const string function_name) {
  py::list retval;
  const auto fit = tick_functions.find(function_name);
  if (fit != tick_functions.end()) {
    for (const auto& fdef : fit->second.input_fields) {
      retval.append(fdef.name);
    }
  }
  return retval;
}

py::list ResultFields(const string function_name) {
  py::list retval;
  const auto fit = tick_functions.find(function_name);
  if (fit != tick_functions.end()) {
    for (const auto& fdef : fit->second.output_fields) {
      retval.append(make_pair(fdef.name, fdef.dtype));
    }
  }
  return retval;
}

PYBIND11_MODULE(taqpy, m) {
  m.doc() = "TAQ Calculator Python API";
  m.def("Execute", &Execute, "Interface to tick-proc server");
  m.def("Describe", &Describe, "Returns json with all supported functions");
  m.def("FunctionList", &FunctionList, "Returns list of supported function names");
  m.def("ArgumentList", &ArgumentList, "Returns list of function's arguments : argument name and datatype");
  m.def("ArgumentNames", &ArgumentNames, "Returns list of function's argument names");
  m.def("ResultFields", &ResultFields, "Returns list of function's return fields : field name and datatype");
#ifdef VERSION_INFO
  m.attr("__version__") = VERSION_INFO;
#else
  m.attr("__version__") = "dev";
#endif
}
