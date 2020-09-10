#include "taq-py.h"

py::list ExecuteVWAP(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const string separator = req_json.get<string>("separator", "|");
  char sep = separator[0];
  const ssize_t input_cnt = req_json.get<ssize_t>("input_cnt", 0);
  vector<function<void(ostream& os, size_t )>> func;
 //auto no_args = [sep](ostream& os, ssize_t i) {os << sep; };
  ostringstream ss;

  py::array_t<str18> arr_symb = kwargs["Symbol"].cast<py::array_t<str18>>();
  func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_symb.at(i) << sep; });

  py::array_t<str12> arr_date = kwargs["Date"].cast<py::array_t<str12>>();
  func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_date.at(i) << sep; });

  py::array_t<str20> arr_stime = kwargs["StartTime"].cast<py::array_t<str20>>();
  func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_stime.at(i) << sep; });

  py::array_t<str6> arr_side = kwargs.contains("Side") ? kwargs["Side"].cast<py::array_t<str6>>() : py::array_t<str6 >();
  if (kwargs.contains("Side")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_side.at(i) << sep; });
  }

  py::array_t<double> arr_lpx = kwargs.contains("LimitPx") ? kwargs["LimitPx"].cast<py::array_t<double>>() : py::array_t<double>();
  if (kwargs.contains("LimitPx")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_lpx.at(i) << sep; });
  }

  py::array_t<str6> arr_flavor = kwargs.contains("Flavor") ? kwargs["Flavor"].cast<py::array_t<str6>>() : py::array_t<str6>();
  if (kwargs.contains("Flavor")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_flavor.at(i) << sep; });
  }

  py::array_t<str20> arr_etime = kwargs.contains("EndTime") ? kwargs["EndTime"].cast<py::array_t<str20>>() : py::array_t<str20>();
  if (kwargs.contains("EndTime")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_etime.at(i) << sep; });
  }

  py::array_t<int> arr_tvol = kwargs.contains("TargetVolume") ? kwargs["TargetVolume"].cast<py::array_t<int>>() : py::array_t<int>();
  if (kwargs.contains("TargetVolume")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_tvol.at(i) << sep; });
  }

  py::array_t<double> arr_tpov = kwargs.contains("TargetPOV") ? kwargs["TargetPOV"].cast<py::array_t<double>>() : py::array_t<double>();
  if (kwargs.contains("TargetPOV")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_tpov.at(i) << sep; });
  }

  py::array_t<int> arr_ticks = kwargs.contains("Ticks") ? kwargs["Ticks"].cast<py::array_t<int>>() : py::array_t<int>();
  if (kwargs.contains("Ticks")) {
    sep = func.size() + 1 < kwargs.size() ? sep : '\n';
    func.push_back([&, sep](ostream& os, ssize_t i) {os << arr_ticks.at(i) << sep; });
  }

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
  py::array_t<int> id((record_cnt));
  py::array_t<int> trd_cnt(record_cnt);
  py::array_t<int> trd_vol((record_cnt));
  py::array_t<double> vwap((record_cnt));
  
  int line_cnt = 0;
  string line;
  vector<string> values;
  while (getline(tcptream, line)) {
    values.clear();
    boost::split(values, line, boost::is_any_of("|"));
    id.mutable_at(line_cnt) = stoi(values[0]);
    trd_cnt.mutable_at(line_cnt) = stoi(values[1]);
    trd_vol.mutable_at(line_cnt) = stoi(values[2]);
    vwap.mutable_at(line_cnt) = stod(values[3]);
    line_cnt++;
  }
  py::list retval;
  retval.append(json_str);
  retval.append(id);
  retval.append(trd_cnt);
  retval.append(trd_vol);
  retval.append(vwap);
  tcptream.close();
  return retval;
}

