#include "taq-py.h"

py::list ExecuteROD(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
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

  tcptream << JsonToString(req_json) << endl;
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
  vector<string_view> values;
  while (getline(tcptream, line)) {
    values.clear();
    Split(values, line, '|');
    StringCopy(ord_id.mutable_at(line_cnt), string(values[0]).c_str(), sizeof(str64));
    minus3.mutable_at(line_cnt) = TextToNumeric<double>(values[1]);
    minus2.mutable_at(line_cnt) = TextToNumeric<double>(values[2]);
    minus1.mutable_at(line_cnt) = TextToNumeric<double>(values[3]);
    zero.mutable_at(line_cnt) = TextToNumeric<double>(values[4]);
    plus1.mutable_at(line_cnt) = TextToNumeric<double>(values[5]);
    plus2.mutable_at(line_cnt) = TextToNumeric<double>(values[6]);
    plus3.mutable_at(line_cnt) = TextToNumeric<double>(values[7]);
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
