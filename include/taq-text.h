#ifndef TAQ_TEXT_INCLUDED
#define TAQ_TEXT_INCLUDED

#include <vector>
#include <string>
#include <string_view>
#include <charconv>

#include "taq-exception.h"

namespace Taq {

template<typename T>
T TextToNumeric(const std::string_view & str) {
  T retval;
  auto [p, ec] = std::from_chars(str.data(), str.data() + str.size(), retval);
  if ((bool)ec)
    throw Exception(ErrorType::InvalidArgument);
  return retval;
}

inline
void Split(std::vector<std::string_view> & result, const std::string_view & str, const char delim) {
  for (size_t first = 0; first < str.size();) {
    size_t second = str.find(delim, first);
    result.emplace_back(str.substr(first, second - first));
    if (second == std::string_view::npos)
      break;
    first = second + 1;
  }
  if (*str.rbegin() == delim) {
    result.emplace_back(&*str.rbegin(), 0);
  }
}

}

#endif
