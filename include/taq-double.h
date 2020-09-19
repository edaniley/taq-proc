
#ifndef TAQ_DOUBLE_INCLUDED
#define TAQ_DOUBLE_INCLUDED

#include <string>
#include <string_view>
#include <limits>

namespace Taq {

class Double {
public:
  static double DEFAULT_PRECISION() { return (0.0000000001); }

  Double() : value_(std::numeric_limits<double>::quiet_NaN()) { }
  Double(double value) : value_(value)  { }
  Double(const std::string& str) {
    try {
      value_ = str.empty() ? std::numeric_limits<double>::quiet_NaN() : std::stod(str);
    }  catch (...) {
      value_ = std::numeric_limits<double>::quiet_NaN();
    }
  }
  Double(const std::string_view& str) : Double(std::string(str.data(), str.size())) {}
  Double (const char* s) {
    value_ = !s || *s == '\0' ? std::numeric_limits<double>::quiet_NaN() : std::stod(s);
  }
  Double& operator = (double value) {
    value_ = value;
    return *this;
  }
  Double& operator = (const Double & rhs ) {
    value_ = rhs.value_;
    return *this;
  }
  operator const double& () const {
    return value_;
  }
  operator double& () {
    return value_;
  }
  const double& Value() const {
    return value_;
  }
  double& Value() {
    return value_;
  }
  void Clear() {
    value_ = Double();
  }
  bool IsZero() const {
    return 0 == value_;
  }
  bool Empty() const {
    return std::isnan(value_);
  }
  bool NotEqual(const Double& to, double eps = DEFAULT_PRECISION()) const {
    double delta = value_ - to.value_;
    return delta <= -eps || eps <= delta;
  }
  bool Equal(const Double& to, double eps = DEFAULT_PRECISION()) const {
    return (!NotEqual(to, eps));
  }
  bool Less(const Double& to, double eps = DEFAULT_PRECISION()) const {
    return value_ + eps < to.value_;
  }
  bool LessOrEqual(const Double& to, double eps = DEFAULT_PRECISION()) const {
    return !(to.Less(*this, eps));
  }
  bool Greater(const Double& to, double eps = DEFAULT_PRECISION()) const {
    return to.Less(*this, eps);
  }
  bool GreaterOrEqual(const Double& to, double eps = DEFAULT_PRECISION()) const {
    return to.LessOrEqual(*this, eps);
  }

  friend std::ostream& operator<< (std::ostream& os, const Double& value) {
    return os << value.value_;
  }
private:
  double value_;
};

}

#endif
