
#include "tick-calc.h"
#include "tick-data.h"

namespace mm = boost::interprocess;
namespace fs = boost::filesystem;

using namespace std;
using namespace Taq;

namespace  tick_calc {

unique_ptr<SecMasterManager> secmaster_manager;
unique_ptr<RecordsetManager<Nbbo>> nbbo_data_manager;
unique_ptr<RecordsetManager<NbboPrice>> nbbo_po_data_manager;
unique_ptr<RecordsetManager<Trade>> trade_data_manager;

void InitializeData(const string & data_dir) {
  secmaster_manager = make_unique<SecMasterManager>(data_dir);
  nbbo_data_manager = make_unique<RecordsetManager<Nbbo>>(data_dir);
  nbbo_po_data_manager = make_unique<RecordsetManager<NbboPrice>>(data_dir);
  trade_data_manager = make_unique<RecordsetManager<Trade>>(data_dir);
}
void CleanupData() {
  nbbo_data_manager.release();
}

tick_calc::SecMasterManager & SecurityMasterManager() {
  return *secmaster_manager;
}

tick_calc::RecordsetManager<Nbbo>& QuoteRecordsetManager() {
  return *nbbo_data_manager;
}

tick_calc::RecordsetManager<NbboPrice>& NbboPoRecordsetManager() {
  return *nbbo_po_data_manager;
}

tick_calc::RecordsetManager<Trade>& TradeRecordsetManager() {
  return *trade_data_manager;
}

void SecMasterManager::trim() {
  if (sec_master_.size() >= max_size_) {
    // attempt to trim stale struct(s)
    vector<pair<Date, const SecMaster*>> tmp;
    for (const auto& x : sec_master_) {
      tmp.push_back(make_pair(x.first, x.second.get()));
    }
    sort(tmp.begin(), tmp.end(), [](pair<Date, const SecMaster*>& l, pair<Date, const SecMaster*>& r) {
      return (l.second->use_cnt_ < r.second->use_cnt_)
        || (l.second->use_cnt_ == r.second->use_cnt_ && l.second->last_used_ < r.second->last_used_);
      });
    size_t to_release = 1 + sec_master_.size() - max_size_;
    for (size_t i = 0; i < to_release; i++) {
      if (tmp[i].second->use_cnt_) {
        break;
      }
      sec_master_.erase(tmp[i].first);
    }
  }
}
/* ===================================================== page ========================================================*/
const SecMaster&  SecMasterManager::Load(Date date) {
  SecMaster* retval = nullptr;
  lock_guard<mutex> lock(mtx_);
  SecMasterTable::iterator found = sec_master_.find(date);
  if (found != sec_master_.end()) {
    retval = found->second.get();
  }
  else {
    trim();
    fs::path file_path = MkDataFilePath(data_dir_, RecordType::SecMaster, date);
    if (false == (fs::exists(file_path) && fs::is_regular_file(file_path))) {
      throw domain_error("Input file not found : " + file_path.string());
    }
    const size_t file_size = (size_t)fs::file_size(file_path);
    if (file_size < sizeof(FileHeader)) {
      throw domain_error("Input file size too small to accomodate header : " + file_path.string());
    }
    mm::file_mapping mmfile(file_path.string().c_str(), mm::read_only);
    mm::mapped_region mmreg(mmfile, mm::read_only);
    const FileHeader& fh = *(FileHeader*)mmreg.get_address();
    if ((sizeof(fh) + fh.symb_cnt * sizeof(Security)) != file_size) {
      throw domain_error("Input file corruption : " + file_path.string());
    }
    auto inserted = sec_master_.insert(make_pair(date, make_unique<SecMaster>(date, fh.symb_cnt, mmfile, mmreg)));
    retval = inserted.first->second.get();
  }
  if (retval) {
    retval->use_cnt_ ++;
    retval->last_used_ = pt::microsec_clock::local_time();
  }
  else {
    throw (domain_error("Too many objects loaded into memory"));
  }
  return *retval;
}

void SecMasterManager::Release(const SecMaster &obj) {
  lock_guard<mutex> lock(mtx_);
  obj.use_cnt_ --;
  assert(obj.use_cnt_ >= 0);
  //sec_master_.erase(obj.date_);
}

}
