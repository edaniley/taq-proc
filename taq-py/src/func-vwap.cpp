#include "taq-py.h"

py::list ExecuteVWAP(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const string separator = req_json.get<string>("separator", "|");
  const ssize_t input_cnt = req_json.get<ssize_t>("input_cnt", 0);
  vector<function<void(ostream& os, size_t )>> func;
  ostringstream ss;

  py::array_t<str18> arr_symb = kwargs["Symbol"].cast<py::array_t<str18>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_symb.at(i) << separator; });

  py::array_t<str12> arr_date = kwargs["Date"].cast<py::array_t<str12>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_date.at(i) << separator; });

  py::array_t<str20> arr_stime = kwargs["StartTime"].cast<py::array_t<str20>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_stime.at(i); });

  py::array_t<str6> arr_side = kwargs.contains("Side") ? kwargs["Side"].cast<py::array_t<str6>>() : py::array_t<str6 >();
  if (kwargs.contains("Side")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_side.at(i); });
  }

  py::array_t<double> arr_lpx = kwargs.contains("LimitPx") ? kwargs["LimitPx"].cast<py::array_t<double>>() : py::array_t<double>();
  if (kwargs.contains("LimitPx")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_lpx.at(i); });
  }

  py::array_t<str6> arr_flavor = kwargs.contains("Flavor") ? kwargs["Flavor"].cast<py::array_t<str6>>() : py::array_t<str6>();
  if (kwargs.contains("Flavor")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_flavor.at(i); });
  }

  py::array_t<str20> arr_etime = kwargs.contains("EndTime") ? kwargs["EndTime"].cast<py::array_t<str20>>() : py::array_t<str20>();
  if (kwargs.contains("EndTime")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_etime.at(i); });
  }

  py::array_t<int> arr_tvol = kwargs.contains("TargetVolume") ? kwargs["TargetVolume"].cast<py::array_t<int>>() : py::array_t<int>();
  if (kwargs.contains("TargetVolume")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_tvol.at(i); });
  }

  py::array_t<double> arr_tpov = kwargs.contains("TargetPOV") ? kwargs["TargetPOV"].cast<py::array_t<double>>() : py::array_t<double>();
  if (kwargs.contains("TargetPOV")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_tpov.at(i); });
  }

  py::array_t<int> arr_ticks = kwargs.contains("Ticks") ? kwargs["Ticks"].cast<py::array_t<int>>() : py::array_t<int>();
  if (kwargs.contains("Ticks")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_ticks.at(i); });
  }

  py::array_t<str96> arr_markouts = kwargs.contains("Markouts") ? kwargs["Markouts"].cast<py::array_t<str96>>() : py::array_t<str96 >();
  if (kwargs.contains("Markouts")) {
    func.push_back([&](ostream& os, ssize_t i) {os << separator << arr_markouts.at(i); });
  }

  tcptream << JsonToString(req_json) << endl;
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
  size_t sets = (fields.size() - 1) /3;
  
  py::array_t<int> id((record_cnt));

  vector<py::array_t<int>> v_trd_cnt;
  v_trd_cnt.reserve(sets);
  vector<py::array_t<int>> v_trd_vol;
  v_trd_vol.reserve(sets);
  vector<py::array_t<double>> v_vwap;
  v_vwap.reserve(sets);
  for (size_t i = 0; i < sets; i ++) {
    v_trd_cnt.emplace_back(record_cnt);
    v_trd_vol.emplace_back(record_cnt);
    v_vwap.emplace_back(record_cnt);
  }

  int line_cnt = 0;
  string line;
  vector<string_view> values;
  while (getline(tcptream, line)) {
    values.clear();
    Split(values, line, '|');
    id.mutable_at(line_cnt) = TextToNumeric<int>(values[0]);
    for (size_t i = 0; i < sets; i ++) {
      v_trd_cnt[i].mutable_at(line_cnt) = TextToNumeric<int>(values[1 + i * 3]);
      v_trd_vol[i].mutable_at(line_cnt) = TextToNumeric<int>(values[2 + i * 3]);
      v_vwap[i].mutable_at(line_cnt) = TextToNumeric<double>(values[3 + i * 3]);
    }
    line_cnt++;
  }
  tcptream.close();

  py::list retval;
  retval.append(json_str);
  retval.append(id);
  for (size_t i = 0; i < sets; i++) {
    retval.append(v_trd_cnt[i]);
    retval.append(v_trd_vol[i]);
    retval.append(v_vwap[i]);
  }
  return retval;
}

