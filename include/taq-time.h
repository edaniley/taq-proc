#ifndef TAQ_TIME_INCLUDED
#define TAQ_TIME_INCLUDED

#include <string>
#include <string_view>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "boost/date_time/local_time_adjustor.hpp"
#include "boost/date_time/c_local_time_adjustor.hpp"

#include "taq-exception.h"

namespace Taq {

using Timestamp = boost::posix_time::ptime;
using Time      = boost::posix_time::time_duration;
using Date      = boost::gregorian::date;
using LocalTzAdjustor = boost::date_time::c_local_adjustor<Timestamp>;

inline Time ZeroTime() {
  return Time(boost::posix_time::seconds(0));
}

inline Time MkTaqTime(const std::string & timestamp) {
  try {
    return  boost::posix_time::hours(                 stoi(timestamp.substr(0,2)) )
          + boost::posix_time::minutes(               stoi(timestamp.substr(2,2)) )
          + boost::posix_time::seconds(               stoi(timestamp.substr(4,2)) )
          + boost::posix_time::nanoseconds(           stoi(timestamp.substr(6,9)) );
  } catch (...) {
    throw Exception(ErrorType::InvalidTimestamp);
  }
}

inline Date MkTaqDate(const std::string & yyyymmdd) {
  try {
    return  boost::gregorian::from_undelimited_string(yyyymmdd);
  } catch (...) {
    throw Exception(ErrorType::InvalidDate);
  }
}

inline Date MkTaqDate(const std::string_view& yyyymmdd) {
  try {
    return  boost::gregorian::from_undelimited_string(std::string(yyyymmdd.data(), yyyymmdd.size()));
  }
  catch (...) {
    throw Exception(ErrorType::InvalidDate);
  }
}

inline Time MkTime(const std::string& time) {
  try {
    return  boost::posix_time::duration_from_string(time);
  } catch (...) {
    throw Exception(ErrorType::InvalidTimestamp);
  }
}

inline Time MkTime(const std::string_view & time) {
  try {
    return  boost::posix_time::duration_from_string(std::string(time.data(), time.size()));
  }
  catch (...) {
    throw Exception(ErrorType::InvalidTimestamp);
  }
}

inline Date MkDate(const std::string& yyyymmdd) {
  try {
    return boost::gregorian::from_string(yyyymmdd);
  } catch (...) {
    throw Exception(ErrorType::InvalidDate);
  }
}

inline Date MkDate(const std::string_view& yyyymmdd) {
  try {
    return boost::gregorian::from_string(std::string(yyyymmdd.data(), yyyymmdd.size()));
  }
  catch (...) {
    throw Exception(ErrorType::InvalidDate);
  }
}

inline Time UtcToTaq(Date date) { // add to UTC time to produce local time (NYSE TAQ)
  Timestamp ts(date, MkTime(std::string("12:00:00")));
  const Timestamp utc_ts(date, MkTime(std::string("12:00:00")));
  const Timestamp est_ts = LocalTzAdjustor::utc_to_local(utc_ts);
  return est_ts - utc_ts;
}

}

#endif
