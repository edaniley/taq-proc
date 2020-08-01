
#ifndef TICK_CALC_SEC_MASTER_INCLUDED
#define TICK_CALC_SEC_MASTER_INCLUDED

#include <iostream>
#include <exception>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <mutex>
#include <memory>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>

#include "taq-proc.h"

namespace mm = boost::interprocess;
namespace fs = boost::filesystem;
namespace pt = boost::posix_time;

using namespace std;
using namespace Taq;

namespace tick_calc {


class SecMasterManager;
class SecMaster {
  friend class SecMasterManager;
  public:
    SecMaster(Date date, size_t sec_no, mm::file_mapping & mmfile, mm::mapped_region & mmreg)
      : date_(date), mmfile_(move(mmfile)), mmreg_(move(mmreg)), use_cnt_(0) {
      Security* start = (Security*)((char*)mmreg_.get_address() + sizeof(FileHeader));
      list_ = vector<Security>(start, start + sec_no);
      for_each(list_.begin(), list_.end(), [&](const auto& sec) {
        by_symb_.insert(make_pair(sec.symb, &sec));
        if (sec.symb != sec.utp_symb) {
          by_symb_.insert(make_pair(sec.utp_symb, &sec));
        }
      });
    }

    ~SecMaster() {
      cout << "~SecMaster date:" << boost::gregorian::to_iso_string(date_) << endl;
    }
    const Security & FindBySymbol(const string& symbol) const {
      auto it = by_symb_.find(symbol);
      if (it == by_symb_.end()) {
        throw(domain_error("Not found date:" + boost::gregorian::to_simple_string(date_) + " symbol:" + symbol));
      }
      return *(it->second);
    }

  private:
    const Date date_;
    mm::file_mapping mmfile_;
    mm::mapped_region mmreg_;
    vector<Security> list_;
    unordered_map<string, const Security*> by_symb_;
    pt::ptime last_used_;
    mutable int use_cnt_;
};

class SecMasterManager {
  public:
    typedef map<Date, unique_ptr<SecMaster>> SecMasterTable;
    SecMasterManager(const string & data_dir, size_t max_size = 2)  : data_dir_(data_dir) , max_size_(max_size) { }
    const SecMaster & Load(Date);
    void Release(const SecMaster &);
  private:
    void trim();
    const string data_dir_;
    const size_t max_size_;
    mutex mtx_;
    SecMasterTable sec_master_;
};

}
#endif