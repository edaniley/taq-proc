include_directories(
  include
  ${NUMPY_INCLUDE_PATH}
  ${PYBIND_INCLUDE_PATH}
  ${PYTHON_INCLUDE_PATH}
)

add_library (
    taqpy MODULE
    src/main.cpp
    src/config.cpp
    src/func-quotes.cpp
    src/func-rod.cpp
    src/func-vwap.cpp
)
