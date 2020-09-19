#include "taq-py.h"

py::list ExecuteNBBO(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const string separator = req_json.get<string>("separator", "|");
  const ssize_t input_cnt = req_json.get<ssize_t>("input_cnt", 0);
  vector<function<void(ostream& os, size_t)>> func;
  ostringstream ss;

  py::array_t<str18> arr_symb = kwargs["Symbol"].cast<py::array_t<str18>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_symb.at(i) << separator; });

  py::array_t<str36> arr_time = kwargs["Timestamp"].cast<py::array_t<str36>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_time.at(i); });

  py::array_t<str96> arr_markouts = kwargs.contains("Markouts") ? kwargs["Markouts"].cast<py::array_t<str96>>() : py::array_t<str96 >();
  if (kwargs.contains("Markouts")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_markouts.at(i); });
  }

  tcptream << JsonToString(req_json) << endl;;
  for (auto i = 0; i < input_cnt; i++) {
    for_each(func.begin(), func.end(), [&](auto f) {f(ss, i); });
    ss << endl;
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

  auto fields = AsVector<string>(response, "output_fields");
  auto sets = (fields.size() - 1) / 3;

  py::array_t<int> id((record_cnt));

  vector<py::array_t<str20>> v_time; // 09:35:28.123456789
  v_time.reserve(sets);
  vector<py::array_t<double>> v_bidp;
  v_bidp.reserve(sets);
  vector<py::array_t<int>> v_bids;
  v_bids.reserve(sets);
  vector<py::array_t<double>> v_askp;
  v_askp.reserve(sets);
  vector<py::array_t<int>> v_asks;
  v_asks.reserve(sets);
  for (auto i = 0; i < sets; i++) {
    v_time.emplace_back(record_cnt);
    auto& last = *v_time.rbegin();
    memset(last.mutable_data(), 0, last.nbytes());
    v_bidp.emplace_back(record_cnt);
    v_bids.emplace_back(record_cnt);
    v_askp.emplace_back(record_cnt);
    v_asks.emplace_back(record_cnt);
  }

  int line_cnt = 0;
  string line;
  vector<string_view> values;
  while (getline(tcptream, line)) {
    values.clear();
    Split(values, line, '|');
    id.mutable_at(line_cnt) = TextToNumeric<int>(values[0]);
    for (auto i = 0; i < sets; i++) {
      StringCopy(v_time[i].mutable_at(line_cnt), string(values[1]).c_str(), sizeof(str20));
      v_bidp[i].mutable_at(line_cnt) = TextToNumeric<double>(values[2]);
      v_bids[i].mutable_at(line_cnt) = TextToNumeric<int>(values[3]);
      v_askp[i].mutable_at(line_cnt) = TextToNumeric<double>(values[4]);
      v_asks[i].mutable_at(line_cnt) = TextToNumeric<int>(values[5]);
    }
    line_cnt++;
  }
  tcptream.close();

  py::list retval;
  retval.append(json_str);
  retval.append(id);
  for (auto i = 0; i < sets; i++) {
    retval.append(v_time[i]);
    retval.append(v_bidp[i]);
    retval.append(v_bids[i]);
    retval.append(v_askp[i]);
    retval.append(v_asks[i]);
  }
  return retval;
}
