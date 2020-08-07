#include "taq-py.h"

py::list ExecuteQuote(const ptree& req_json, ip::tcp::iostream& tcptream, const py::kwargs& kwargs) {
  const string separator = req_json.get<string>("separator", "|");
  const ssize_t input_cnt = req_json.get<ssize_t>("input_cnt", 0);
  vector<function<void(ostream& os, size_t)>> func;
  ostringstream ss;

  py::array_t<str18> arr_symb = kwargs["Symbol"].cast<py::array_t<str18>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_symb.at(i) << separator; });

  py::array_t<str36> arr_time = kwargs["Timestamp"].cast<py::array_t<str36>>();
  func.push_back([&](ostream& os, ssize_t i) {os << arr_time.at(i) << endl; });

  tcptream << JsonToString(req_json) << endl;;
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
