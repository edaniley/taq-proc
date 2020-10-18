#ifndef TICK_OUTPUT_BUILDER_INCLUDED
#define TICK_OUTPUT_BUILDER_INCLUDED


#include <vector>
#include <algorithm>
#include <functional>
#include <sstream>
#include "tick-exec.h"

using namespace std;
using namespace Taq;


namespace tick_calc {

struct OutputRecordTracker {
  OutputRecordTracker(OutputRecordset::const_iterator start, const OutputRecordset::const_iterator end, string &record_filler)
    : current(start), end(end), to_advance(false), record_filler(move(record_filler)) {}
  OutputRecordset::const_iterator current;
  const OutputRecordset::const_iterator end;
  bool to_advance;
  const string record_filler;
};

class OutputRecordBuilder {
public:
  void Prepare(const vector<shared_ptr<ExecutionPlan>> exec_plans) {
    for (auto & plan : exec_plans) {
      string record_filler = string(plan->output_fields.size() - 1, '|');
      trakers.emplace_back(plan->output_records.begin(), plan->output_records.end(), record_filler);
    }
  }

  string Build() {
    ostringstream ss;
    int next_id = NextID();
    ss << next_id;
    for (auto & it : trakers) {
      if (it.current != it.end && it.current->id == next_id) {
        ss << '|' << it.current->data;
        it.to_advance = true;
      } else {
        ss << '|' << it.record_filler;
      }
    }
    ss << endl;
    return ss.str();
  }

  void Advance() {
    for (auto & it : trakers) {
      if (it.to_advance) {
        ++ it.current;
        it.to_advance = false;
      }
    }
  }

private:
  int NextID() {
    int next_id = numeric_limits<int>::max();
    for (const auto & it : trakers) {
      next_id = it.current == it.end ? next_id : min(it.current->id, next_id);
    }
    return next_id;
  }

  vector<OutputRecordTracker> trakers;
};

}

#endif
