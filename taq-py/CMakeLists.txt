set(NUMPY_INCLUDE_PATH  /usr/local/lib64/python3.8/site-packages/numpy/core/include )
set(PYBIND_INCLUDE_PATH /home/edaniley/Toolbox/pybind11/include )
set(PYTHON_INCLUDE_PATH /usr/include/python3.8 )

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
)
