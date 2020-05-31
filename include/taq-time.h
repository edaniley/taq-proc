#ifndef TAQ_TIME_INCLUDED
#define TAQ_TIME_INCLUDED

#include <string>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace Taq {

typedef boost::posix_time::time_duration Time;
typedef boost::gregorian::date Date;

inline Time MkTaqTime(const std::string & timestamp) {
  return  boost::posix_time::hours(                 stoi(timestamp.substr(0,2)) )
        + boost::posix_time::minutes(               stoi(timestamp.substr(2,2)) )
        + boost::posix_time::seconds(               stoi(timestamp.substr(4,2)) )
        // + boost::posix_time::microseconds( lround ( stod(timestamp.substr(6,9)) / 1000)
        + boost::posix_time::nanoseconds(           stoi(timestamp.substr(6,9)) );
}

inline Date MkTaqDate(const std::string & yyyymmdd) {
  return  boost::gregorian::from_undelimited_string(yyyymmdd);
}

inline Time MkTime(const std::string& time) {\
  return  boost::posix_time::duration_from_string(time);
}

inline Date MkDate(const std::string& yyyymmdd) {
  return  boost::gregorian::from_string(yyyymmdd);
}


}

#endif
