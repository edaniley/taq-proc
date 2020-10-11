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
    src/exec.cpp
)
