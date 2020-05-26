#ifndef TICK_CALC_INCLUDED
#define TICK_CALC_INCLUDED

#include <string>
#include <vector>
#include <algorithm>
#include <functional>
#include <iterator>
#include <map>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem.hpp>

#include "taq-proc.h"
#include "tick-calc-secmaster.h"

namespace mm = boost::interprocess;
namespace fs = boost::filesystem;
namespace pt = boost::posix_time;

using namespace std;
using namespace Taq;

namespace tick_calc {

/* ===================================================== page ========================================================*/
  template <typename T>
  class SortedConstVector {
  public:
    SortedConstVector() = default;
    SortedConstVector(const T* base, size_t record_count) : base_(base), record_count_(record_count) {}
    const T * begin() const {return base_;}
    const T * end()   const {return base_ + record_count_;}
    size_t    size()  const {return record_count_;}
    size_t distance(const T* left, const T*right) const {return right - left;}

    const T* upper_bound(const T* left, const T* right, Time time) const {
      ptrdiff_t lo = 0, hi = right - left;
      while (lo < hi) {
        ptrdiff_t mid = lo + (hi - lo) / 2;
        if (time >= left[mid].time) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      return left + lo;
    }

    const T* lower_bound(const T* left, const T* right, Time time) const {
      ptrdiff_t lo = 0, hi = right - left;
      while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (time <= left[mid].time) {
          hi = mid;
        }
        else {
          lo = mid + 1;
        }
      }
      return left + lo;
    }
  private:
    const T * base_;
    size_t record_count_;
  };
/* ===================================================== page ========================================================*/
template <typename T>
class SymbolRecordset{
public:
  SymbolRecordset(SymbolRecordset &&) = delete;
  SymbolRecordset(const SymbolRecordset &) = delete;
  SymbolRecordset(mm::mapped_region & mmreg) : mmreg_(move(mmreg)) {
    const T* data = (T*)mmreg_.get_address();
    const size_t record_count = mmreg_.get_size() / sizeof(T);
    records = move(SortedConstVector<T>(data, record_count));
  }
  SortedConstVector<T> records;
private:
  mm::mapped_region mmreg_;
};
/* ===================================================== page ========================================================*/
template <typename T>
class DayRecordset{
  public:
    DayRecordset(Date date, mm::file_mapping& mmfile, mm::mapped_region& mmreg_header, mm::mapped_region& mmreg_map)
      : date_(date), mmfile_(move(mmfile)), mmreg_hdr_(move(mmreg_header)), mmreg_map_(move(mmreg_map)), use_cnt_(0) {
      const size_t symb_cnt = ((FileHeader*)mmreg_hdr_.get_address())->symb_cnt;
      const SymbolMap* start = (SymbolMap*)mmreg_map_.get_address();
      for (size_t i = 0; i < symb_cnt; i ++) {
        const SymbolMap* p = start + i;
        symb_map_.insert(make_pair(p->symb, p));
      }
    }

    ~DayRecordset() {
      cout << "~DayRecordset date:" << boost::gregorian::to_iso_string(date_) << endl;
    }

    const SymbolRecordset<T>* Find(const string& symbol) {
      SymbolRecordset<T>* retval = nullptr;
      auto found = by_symb_.find(symbol);
      if (found == by_symb_.end()) {
        auto symb = symb_map_.find(symbol);
        if (symb != symb_map_.end()) {
          const size_t offset = sizeof(FileHeader) + sizeof(T) * (symb->second->start - 1);
          const size_t length = sizeof(T) * (symb->second->end - symb->second->start + 1);
          mm::mapped_region mmreg_map(mmfile_, mm::read_only, offset, length);
          auto inserted = by_symb_.insert(make_pair(symbol, make_unique<SymbolRecordset<T>>(mmreg_map)));
          retval = inserted.first->second.get();
        }
      } else {
        retval = found->second.get();
      }
      return retval;
    }
    void Release(const string& symbol) {
      by_symb_.erase(symbol);
    }

    pt::ptime & LastUsed() {return last_used_;}
    pt::ptime LastUsed() const { return last_used_; }
    int & UseCount() { return use_cnt_; }
    int UseCount() const { return use_cnt_;}

  private:
    const Date date_;
    mm::file_mapping mmfile_;
    mm::mapped_region mmreg_hdr_;
    mm::mapped_region mmreg_map_;
    map<string, const SymbolMap *> symb_map_;
    map<string, unique_ptr<SymbolRecordset<T>>> by_symb_;
    pt::ptime last_used_;
    int use_cnt_;
};
/* ===================================================== page ========================================================*/
template <typename T>
class RecordsetManager {
public:
  typedef map<Date, unique_ptr<DayRecordset<T>>> DailyQuoteTable;
  RecordsetManager(const string& data_dir, size_t max_size = 2) : data_dir_(data_dir), max_size_(max_size) { }

  const SymbolRecordset<T>& LoadSymbolRecordset(Date date, const string symbol) {
    lock_guard<mutex> lock(mtx_);
    DayRecordset<T>& day_recordset = load(date, symbol[0]);
    const SymbolRecordset<T>* symbol_recordset = day_recordset.Find(symbol);
    if (!symbol_recordset) {
      throw(domain_error("Not found date:" + boost::gregorian::to_simple_string(date) + " symbol:" + symbol));
    }
    day_recordset.UseCount()++;
    day_recordset.LastUsed() = pt::microsec_clock::local_time();
    return *symbol_recordset;
  }

  void UnloadSymbolRecordset(Date date, const string symbol) {
    lock_guard<mutex> lock(mtx_);
    auto found = daily_quotes_.find(date);
    if (found != daily_quotes_.end()) {
      found->second->Release(symbol);
      found->second->UseCount()--;
    }
  }

private:
  void trim() {
    if (daily_quotes_.size() >= max_size_) {
      // attempt to trim stale struct(s)
      vector<pair<Date, const DayRecordset<T>*>> tmp;
      for (const auto& x : daily_quotes_) {
        tmp.push_back(make_pair(x.first, x.second.get()));
      }
      sort(tmp.begin(), tmp.end(), [](pair<Date, const DayRecordset<T>*>& l, pair<Date, const DayRecordset<T>*>& r) {
        return (l.second->UseCount() < r.second->UseCount())
          || (l.second->UseCount() == r.second->UseCount() && l.second->LastUsed() < r.second->LastUsed());
        });
      int to_release = 1 + daily_quotes_.size() - max_size_;
      for (int i = 0; i < to_release; i++) {
        if (tmp[i].second->UseCount()) {
          break;
        }
        daily_quotes_.erase(tmp[i].first);
      }
    }
  }
  DayRecordset<T>& load(Date date, char symbol_group) {
    DayRecordset<T>* retval = nullptr;
    auto found = daily_quotes_.find(date);
    if (found != daily_quotes_.end()) {
      retval = found->second.get();
    }
    else {
      trim();
      fs::path file_path = MkDataFilePath(data_dir_, RecordTypeFromString(typeid(T).name()), date, symbol_group);
      if (false == (fs::exists(file_path) && fs::is_regular_file(file_path))) {
        throw domain_error("Input file not found : " + file_path.string());
      }
      const size_t file_size = (size_t)fs::file_size(file_path);
      if (file_size < sizeof(FileHeader)) {
        throw domain_error("Input file size too small to accomodate header : " + file_path.string());
      }
      mm::file_mapping mmfile(file_path.string().c_str(), mm::read_only);
      mm::mapped_region mmreg_header(mmfile, mm::read_only, 0, sizeof(FileHeader));
      const FileHeader& file_header = *(FileHeader*)mmreg_header.get_address();
      if ((sizeof(file_header) + file_header.symb_cnt * sizeof(SymbolMap) + file_header.rec_cnt * sizeof(T)) != file_size) {
        throw domain_error("Input file corruption : " + file_path.string());
      }
      size_t off = sizeof(FileHeader) + file_header.rec_cnt * sizeof(T);
      size_t siz = file_size - off;
      mm::mapped_region mmreg_map(mmfile, mm::read_only, off, siz);
      auto inserted = daily_quotes_.insert(make_pair(date, make_unique<DayRecordset<T>>(date, mmfile, mmreg_header, mmreg_map)));
      retval = inserted.first->second.get();
    }
    if (!retval) {
      throw (domain_error("Too many objects loaded into memory"));
    }
    return *retval;
  }

  RecordsetManager() = delete;
  const string data_dir_;
  const size_t max_size_;
  mutex mtx_;
  map<Date, unique_ptr<DayRecordset<T>>> daily_quotes_;
};

struct AppContext {
  std::string data_dir;
  uint16_t port;
};

void NetInitialize(AppContext &);
void NetPoll(AppContext &);
}
#endif

