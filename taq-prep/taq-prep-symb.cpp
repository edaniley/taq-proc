
#include <string>
#include <map>
#include <vector>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include "boost-algorithm-string.h"
#include "taq-prep.h"

using namespace std;
using namespace Taq;
namespace fs = boost::filesystem;
namespace mm = boost::interprocess;

namespace taq_prep {

static void GenSuffix(map<string, string>& mapping, const string& nyse, const string& nasd) {
  for (int i = 0; i < 26; i++) {
    string key = nyse; key.append(1, char('A' + i));
    string val = nasd; val.append(1, char('A' + i));
    mapping.insert(make_pair(key, val));
  }
}

static map<string, string> Initilaize() {
  map<string, string> mapping = {
    {"PR" , "-"},
    {"WS" , "+"},
    {"WD" , "$"},
    {"CL" , "*"},
    {"WI" , "#"},
    {"EC" , "!"},
    {"PP" , "@"},
    {"CV" , "%"},
    {"RT" , "^"},
    {"U" , "="},
    {"TEST" , "~"}
  };
  GenSuffix(mapping, "PR", "-");
  GenSuffix(mapping, "", ".");
  GenSuffix(mapping, "WS", "+");
  return mapping;
}

static const map<string, string> adhoc_suffix_map = { {"PRCL","-*"}, {"PREC","-!"}, {"",""} };
static const map<string, string> suffix_map = Initilaize();

pair<string, string> GetLongestSub(const string& suffix) {
  string key;
  size_t key_length = 0;
  for (auto it : suffix_map) {
    const string& current_key = it.first;
    size_t current_key_length = current_key.length();
    if (suffix.substr(0, current_key_length) == current_key) {
      if (current_key_length > key_length) {
        key = current_key;
        key_length = current_key.length();
      }
    }
  }
  string rem = suffix.substr(key_length);
  auto vit = suffix_map.find(key);
  string val = vit != suffix_map.end() ? vit->second : string("");
  return make_pair(rem, val);
}

static string NyseSuffixToNasdaq(const string& suffix) {
  auto it = adhoc_suffix_map.find(suffix);
  if (it != adhoc_suffix_map.end()) {
    return it->second;
  }
  pair<string, string> ret = GetLongestSub(suffix);
  return ret.second + NyseSuffixToNasdaq(ret.first);
}

string CtaToUtp(const string & cta_symbol) {
  vector<string> symb;
  boost::split(symb, cta_symbol, boost::is_any_of(" "));
  return symb.size() == 1 ? cta_symbol : symb[0] + NyseSuffixToNasdaq(symb[1]);
}


static mm::file_mapping mmfile;
static mm::mapped_region mmreg;
static map<string, Security *> security_by_symbol;

void SecMasterLoad(AppContext & ctx) {
  security_by_symbol.clear();
  auto file_path = MkDataFilePath(ctx.output_dir, RecordType::SecMaster, MkTaqDate(ctx.date));
  if (false == (fs::exists(file_path) && fs::is_regular_file(file_path))) {
    throw domain_error("SecMaster file not found : " + file_path.string());
  }
  const size_t file_size = (size_t)fs::file_size(file_path);
  if (file_size < sizeof(FileHeader)) {
    throw domain_error("Input file size is too small : " + file_path.string());
  }
  mmfile = mm::file_mapping(file_path.string().c_str(), mm::read_write);
  mmreg = mm::mapped_region(mmfile, mm::read_write);
  const void *base = mmreg.get_address();
  const FileHeader &fh = *(FileHeader*)base;
  if (fh.type == RecordType::SecMaster) {
    Security* symbols = (Security *)((
    char *)base + sizeof(FileHeader));
    for (int i = 0; i < fh.symb_cnt; i++) {
      const Security& sec = symbols[i];
      security_by_symbol.insert(make_pair(sec.symb, &symbols[i]));
      if (strcmp(sec.symb, sec.utp_symb)) {
        security_by_symbol.insert(make_pair(sec.utp_symb, &symbols[i]));
      }
    }
  }
}

void SecMasterFlush() {
  mmreg.flush(0, mmreg.get_size(), false);
}

Security * GetSecurity(const std::string symbol) {
  auto it = security_by_symbol.find(symbol);
  return it != security_by_symbol.end() ? it->second : nullptr;
}


}
