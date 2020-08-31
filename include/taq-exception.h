#ifndef TAQ_EXEPTION_INCLUDED
#define TAQ_EXEPTION_INCLUDED

#include <string>
#include <exception>

namespace Taq {

enum class ErrorType {
  OK, DataNotFound, MissingSymbol, MissingArgument, InvalidArgument,
  InvalidTimestamp, InvalidDate,
  InvalidSide, InvalidQuantity, InvalidPrice
};

class Exception : public std::runtime_error {
public:
  Exception(ErrorType error_type) noexcept
    : std::runtime_error(""), error_type_(error_type) {}
  Exception(ErrorType error_type, const std::string& message) noexcept
    : std::runtime_error(message), error_type_(error_type) {}
  ErrorType errtype() const { return error_type_; }
private:
  const ErrorType error_type_;
};

inline const char * ErrorToString(ErrorType error_type) {
  switch (error_type) {
  case ErrorType::DataNotFound: return "DataNotFound";
  case ErrorType::MissingSymbol: return "MissingSymbol";
  case ErrorType::InvalidArgument: return "InvalidArgument";
  case ErrorType::InvalidTimestamp: return "InvalidTimestamp";
  case ErrorType::InvalidDate: return "InvalidDate";
  case ErrorType::InvalidSide: return "InvalidSide";
  case ErrorType::InvalidQuantity: return "InvalidQuantity";
  case ErrorType::InvalidPrice: return "InvalidPrice";
  case ErrorType::OK: return "Success";
  }
  return "Unknown";
}

}

#endif
