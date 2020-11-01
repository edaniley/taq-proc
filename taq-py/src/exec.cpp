#include <set>
#include "taq-py.h"

extern map<string, FunctionFieldsDef> tick_arguments;

struct InputArg {
  InputArg(const string &name, const FieldsDef& fdef) : name(name), position(0), fdef(fdef) {}
  string name;            // input argument name
  size_t position;        // 1-based index in python argument list;
  const FieldsDef& fdef;  // argument name and data type expected by TDP
};

// list of pairs input argument name -> TDP function argument definition
using InputArgList = vector<InputArg>;

// function to add field to request record stream; ssize_t is index in numpy array
using WriteFieldFn = function<void(ostream &, ssize_t)>;

// function to convert input field from text to required data type and insert value
// into result numpy array using ssize_t is index
using ReadFieldFn = function<void(const string_view &, ssize_t)>;

// returns pair of strings
// 1. returns pair of input argument name and corresponding TDP function argument name
//    reads from one of JSON entries in function's entry in "argument_mapping" section
// 2. returns result field name and datatype from "result_fields" section in result JSON
static pair<string, string> ReadStringPair(const Json& entry) {
  vector<string> args;
  for (const auto& it : entry) {
    if (it.second.empty()) {
      args.emplace_back(it.second.data());
    }
  }
  return args.size() == 2 ? make_pair(move(args[0]), move(args[1])) : pair<string, string>();
}

// returns table of TDP function arguments' definitions indexed by input argument name
static InputArgList MakeArgToArgMap(const Json& arguments, const FunctionFieldsDef & tdp_args) {
  InputArgList retval;
  for (const auto& arg : arguments) {
    if (!arg.second.empty()) {
      pair<string, string> arg2arg = ReadStringPair(arg.second);
      auto fit = tdp_args.find(arg2arg.first);
      if (fit != tdp_args.end()) {
        retval.emplace_back(move(arg2arg.second), fit->second);
      } else {
        throw domain_error("Invalid argument name : " + arg2arg.second);
      }
    }
  }
  return retval;
}

// returns list of requested functions with corresponding argument mapping
static vector<InputArgList> InputArgumentMapping(const Json& root) {
  vector<InputArgList> retval;
  for (auto& it : root.get_child("argument_mapping")) {
    const string function_name = it.second.get<string>("function");
    auto fit = tick_arguments.find(function_name);
    if (fit != tick_arguments.end()) {
      auto func_args = MakeArgToArgMap(it.second.get_child("arguments"), fit->second);
      retval.emplace_back(move(func_args));
    } else {
      throw domain_error("Unknown function : " + function_name);
    }
  }
  return retval;
}

// return set of used arguments, ignore possible noise from Python client
static set<string> MakeInuseInputArgsList(const vector<InputArgList>& input_args_mapping) {
  set<string> args_used;
  for (const auto& arg_defs : input_args_mapping) {
    for (auto& arg_def : arg_defs) {
      args_used.emplace(arg_def.name);
    }
  }
  return args_used;
}

// update both containers:
//  input_args get data types and sizes
//  input_args_mapping get TDP argument positions in kwargs
static void UpdateInputArgsInfo(vector<tuple<string, string, size_t>> & input_args,
                                vector<InputArgList> & input_args_mapping) {
  for (auto& arg_defs : input_args_mapping) {
    for (auto& arg_def : arg_defs) {
      for (size_t i = 0; i < input_args.size(); i++) {
        const string& name = get<0>(input_args[i]);
        if (name == arg_def.name) {
          string& data_type = get<1>(input_args[i]);
          size_t& data_size = get<2>(input_args[i]);
          if (data_type.empty()) {
            data_type = arg_def.fdef.ctype;
            data_size = arg_def.fdef.size;
          }
          else if (data_type != arg_def.fdef.ctype || data_size != arg_def.fdef.size) {
            throw domain_error("Argument data type missmatch : " + arg_def.fdef.name);
          }
          arg_def.position = i + 1;
          break;
        }
      }
    }
  }
}

static string MakeNewJson(const Json& root, vector<InputArgList>& input_args_mapping) {
  stringstream ss;
  ss << "{\"request_id\":\"" << root.get<string>("request_id")
     << "\",\"input_cnt\":" << root.get<int>("input_cnt")
     << ",\"input_sorted\":" << (root.get<bool>("input_sorted") ? "true" : "false")
     << ",\"time_zone\":\"" << root.get<string>("time_zone")
     << "\",\"argument_mapping\":[";
  size_t i = 0;
  for (auto it : root.get_child("argument_mapping")) {
    const InputArgList & args = input_args_mapping[i++];
    ss << (i == 1 ? "{" : "},{") << "\"function\":\"" << it.second.get<string>("function")
       << "\",\"alias\":\"" << it.second.get<string>("alias", "")
       << "\",\"arguments\":[";
    for (size_t j = 0; j < args.size(); ++ j) {
      ss << (j == 0 ? "[\"" : "],[\"") << args[j].fdef.name << "\"," << args[j].position;
    }
    ss << "]]";
  }
  ss << "}]}";
  return ss.str();
}

template<int SIZE>
WriteFieldFn BindCharArray(const py::kwargs& kwargs, const string& arg_name, char sep) {
  py::array_t<char[SIZE]> array = kwargs[arg_name.c_str()].cast<py::array_t<char[SIZE]>>();
  return [array, sep](ostream& os, ssize_t i) {os << array.at(i) << sep; };
}

template<typename T>
WriteFieldFn BindNumericArray(const py::kwargs& kwargs, const string& arg_name, char sep) {
  py::array_t<T> array = kwargs[arg_name.c_str()].cast<py::array_t<T>>();
  return [array, sep](ostream& os, ssize_t i) {os << array.at(i) << sep; };
}

static WriteFieldFn BindInputRoutines(const py::kwargs& kwargs, char sep,
                                const string &arg_name, const string& arg_type, size_t arg_size) {
  static const WriteFieldFn nop = [](ostream& os, ssize_t i){};
  if (arg_type == typeid(char).name()) {
    switch (arg_size) {
      case 6: return BindCharArray<6>(kwargs, arg_name, sep);
      case 12: return BindCharArray<12>(kwargs, arg_name, sep);
      case 18: return BindCharArray<18>(kwargs, arg_name, sep);
      case 20: return BindCharArray<20>(kwargs, arg_name, sep);
      case 36: return BindCharArray<36>(kwargs, arg_name, sep);
      case 64: return BindCharArray<64>(kwargs, arg_name, sep);
      case 96: return BindCharArray<96>(kwargs, arg_name, sep);
    }
  }
  else if (arg_type == typeid(int).name()) {
    return BindNumericArray<int>(kwargs, arg_name, sep);
  }
  else if (arg_type == typeid(double).name()) {
    return BindNumericArray<double>(kwargs, arg_name, sep);
  }
  return nop;
}

// function to convert result field from text to required data type
// and add it to result numpy array with ssize_t as index
using WriteResultFieldFn = function<void(const string_view & result_field, size_t idx)>;
// function to add result numpy array to the final result list to be returned to python caller
using ReturnResultFn = function<void(py::list & output_list)>;

template<int SIZE>
pair<WriteResultFieldFn, ReturnResultFn> BindTextResultRoutines(size_t size) {
 auto * ptr = new py::array_t<char[SIZE]>(size);
  memset(ptr->mutable_data(), 0, ptr->nbytes());
  return pair<WriteResultFieldFn, ReturnResultFn>(
    [ptr] (const string_view& str, size_t idx) {
      memcpy(ptr->mutable_at(idx), str.data(), min(str.size(), (size_t)SIZE));
    },
    [ptr] (py::list& ret) {
      ret.append(*ptr); // increments ref. count
      delete ptr;       // NB! decrements ref. count
    }
  );
}

template<typename T>
pair<WriteResultFieldFn, ReturnResultFn> BindNumericResultRoutines(size_t size) {
  auto * ptr = new py::array_t<T>(size);
  return pair<WriteResultFieldFn, ReturnResultFn>(
    [ptr](const string_view& str, size_t idx) {
      ptr->mutable_at(idx) = TextToNumeric<T>(str);
    },
    [ptr](py::list& ret) {
      ret.append(*ptr); // increments ref. count
      delete ptr;       // NB! decrements ref. count
    }
  );
}

static pair<WriteResultFieldFn, ReturnResultFn> BindResultRoutines(const string& arg_type, size_t result_size) {
  static const pair<WriteResultFieldFn, ReturnResultFn> nop = {[] (const string_view &, size_t) {}, [] (py::list &) {} };
  if (arg_type[0] == 'a') {
    int arg_size = stoi(arg_type.substr(1));
    switch (arg_size) {
      case 6: return BindTextResultRoutines<6>(result_size);
      case 12: return BindTextResultRoutines<12>(result_size);
      case 18: return BindTextResultRoutines<18>(result_size);
      case 20: return BindTextResultRoutines<20>(result_size);
      case 36: return BindTextResultRoutines<36>(result_size);
      case 64: return BindTextResultRoutines<64>(result_size);
      case 96: return BindTextResultRoutines<96>(result_size);
    }
  }
  else if (arg_type == "double") {
    return BindNumericResultRoutines<double>(result_size);
  }
  else if (arg_type == "int") {
    return BindNumericResultRoutines<int>(result_size);
  }
  return nop;
}

py::list ExecuteImlp(const Json& request, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  // for every requested function build argument map:
  //  each input argument -> TDP-expected argument name and type
  vector<InputArgList> input_args_mapping = InputArgumentMapping(request);

  // initialize caller's arguments
  vector<tuple<string, string, size_t>> input_args;// tuple: input argument name, data type, size
  set<string> args_inuse = MakeInuseInputArgsList(input_args_mapping);
  for (auto it : kwargs) {
    string arg_name = it.first.cast<py::str>();
    if (args_inuse.find(arg_name) != args_inuse.end()) {
      input_args.emplace_back(move(arg_name), "", 0);
    }
  }

  // update both containers:
  //  input_args get data types and sizes
  //  input_args_mapping get TDP argument positions in kwargs
  UpdateInputArgsInfo(input_args, input_args_mapping);

  const ssize_t input_cnt = request.get<ssize_t>("input_cnt", 0);
  // bind each input array with its output routine
  vector<WriteFieldFn> input_func;
  for (size_t i = 0;  i < input_args.size(); ++ i) {
    const char sep = (i == input_args.size()-1) ? '\n' : '|';
    const string & arg_name = get<0>(input_args[i]);
    const string & arg_type = get<1>(input_args[i]);
    size_t arg_size = get<2>(input_args[i]);
    input_func.push_back(BindInputRoutines(kwargs, sep, arg_name, arg_type, arg_size));
  }
  // build new request JSON; in "argument_mapping" section replace
  //  each (input argument, TDP argument) pair
  //  with (TDP argument, field's position in request record)
  auto xx = MakeNewJson(request, input_args_mapping);
  tcptream << xx << endl;;
  //tcptream << MakeNewJson(request, input_args_mapping) << endl;;
  ostringstream ss;
  for (auto i = 0; i < input_cnt; i++) {
    for_each(input_func.begin(), input_func.end(), [&](auto fn) {fn(ss, i); });
    if (ss.str().size() > 64 * 1024) {
      tcptream << ss.str();
      ss.str("");
      ss.clear();
    }
  }
  tcptream << ss.str();

  string line;
  std::getline(tcptream, line);
  Json response = StringToJson(line);
  const size_t record_cnt = response.get<size_t>("output_records", 0);

  vector<pair<string, string>> result_fields;
  result_fields.emplace_back("ID", "int");
  for (auto& function_results : response.get_child("result_fields")) {
    for (auto& it1: function_results.second) {
      result_fields.emplace_back(ReadStringPair(it1.second));
    }
  }

  vector<pair<WriteResultFieldFn, ReturnResultFn>> result_func;
  for (const auto &fld : result_fields) {
    result_func.push_back(BindResultRoutines(fld.second, record_cnt));
  }

  vector<string_view> values;
  for (size_t line_cnt = 0; line_cnt < record_cnt && getline(tcptream, line); ++line_cnt) {
    values.clear();
    Split(values, line, '|');
    assert(values.size() == result_func.size());
    for (size_t i = 0; i < result_func.size(); ++ i) {
      result_func[i].first(values[i], line_cnt);
    }
  }

  py::list retval;
  // fix-up result JSON by adding runtime_summary (last received line from TDP)
  getline(tcptream, line);
  Json runtime_summary = StringToJson(line);
  response.add_child("runtime_summary", runtime_summary);
  retval.append(JsonToString(response));

  for (size_t i = 0; i < result_func.size(); ++i) {
    result_func[i].second(retval);
  }

  tcptream.close();
  return retval;
}