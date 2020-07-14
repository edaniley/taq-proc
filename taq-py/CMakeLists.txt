cmake_minimum_required(VERSION 2.8.12)
project(taqpy)

set(CMAKE_VERBOSE_MAKEFILE ON)

if(MSVC)
  set(WINVER_10 "-D_WIN32_WINNT=0x0A00")
  set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${WINVER_10}")
  set(BOOST_INCLUDE_PATH  C:/Toolbox/boost/boost_1_72_0 )
  set(BOOST_LIB_PATH      C:/Toolbox/boost/boost_1_72_0/stage/lib )
  set(NUMPY_INCLUDE_PATH  C:/Python/Python37/lib/site-packages/numpy/core/include )
else()
  set(BOOST_INCLUDE_PATH  /home/edaniley/Toolbox/boost_1_72_0 )
  set(BOOST_LIB_PATH      /home/edaniley/Toolbox/boost_1_72_0/stage/lib )
  set(NUMPY_INCLUDE_PATH  /usr/local/lib64/python3.8/site-packages/numpy/core/include )
endif()

include_directories(
  include
  ${BOOST_INCLUDE_PATH}
  ${NUMPY_INCLUDE_PATH}
)

link_directories (
  ${BOOST_LIB_PATH}
)

if(LINUX)
  add_library(            boost_iostreams   STATIC IMPORTED)
  set_target_properties(  boost_iostreams   PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_iostreams.a")
  add_library(            boost_date_time   STATIC IMPORTED)
  set_target_properties(  boost_date_time   PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_date_time.a")

  link_libraries(
    boost_iostreams
    boost_date_time
    )
endif()

add_subdirectory(pybind11)
pybind11_add_module(
    taqpy
    src/main.cpp
    src/config.cpp
    src/func-quotes.cpp
    src/func-rod.cpp
)
