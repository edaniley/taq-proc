cmake_minimum_required(VERSION 3.7)
project(taq-proc C CXX ASM-ATT)

option(BOOST_STATIC_LINK "Static link boost dependencies" ON)
#set(CMAKE_VERBOSE_MAKEFILE ON)

set(CXX_FLAGS "-std=c++17 -Wall -Werror -Wpedantic")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CXX_FLAGS_EXTRA "-O3")
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CXX_FLAGS_EXTRA "-O0 -ggdb")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS} ${CXX_FLAGS_EXTRA}")

add_definitions(-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)

# allow cmake params to be set from environment variables
if(DEFINED ENV{BOOST_INCLUDE_PATH})
  set(BOOST_INCLUDE_PATH  $ENV{BOOST_INCLUDE_PATH} )
endif()

if(DEFINED ENV{BOOST_LIB_PATH})
  set(BOOST_LIB_PATH      $ENV{BOOST_LIB_PATH} )
endif()

if(DEFINED ENV{NUMPY_INCLUDE_PATH})
  set(NUMPY_INCLUDE_PATH  $ENV{NUMPY_INCLUDE_PATH} )
endif()

if(DEFINED ENV{PYBIND_INCLUDE_PATH})
  set(PYBIND_INCLUDE_PATH $ENV{PYBIND_INCLUDE_PATH} )
endif()

if(DEFINED ENV{PYTHON_INCLUDE_PATH})
  set(PYTHON_INCLUDE_PATH $ENV{PYTHON_INCLUDE_PATH} )
endif()

# apply default values if build params not set
if(NOT BOOST_INCLUDE_PATH)
  set(BOOST_INCLUDE_PATH  /usr/local/astral/boost_1_72_0 )
endif()

if(NOT BOOST_LIB_PATH)
  set(BOOST_LIB_PATH      /usr/local/astral/boost_1_72_0/stage/lib )
endif()

if(NOT NUMPY_INCLUDE_PATH)
  set(NUMPY_INCLUDE_PATH  /usr/local/lib/python3.6/site-packages/numpy/core/include )
endif()

if(NOT PYBIND_INCLUDE_PATH)
  set(PYBIND_INCLUDE_PATH /usr/local/astral/pybind11/include )
endif()

if(NOT PYTHON_INCLUDE_PATH)
  set(PYTHON_INCLUDE_PATH /usr/include/python3.8 )
endif()


include_directories(
    include
    ${BOOST_INCLUDE_PATH}
)

add_library(            boost_program_options   STATIC IMPORTED)
add_library(            boost_filesystem        STATIC IMPORTED)
add_library(            boost_date_time         STATIC IMPORTED)
add_library(            boost_iostreams         STATIC IMPORTED)

if(BOOST_STATIC_LINK)
  set_target_properties(  boost_program_options   PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_program_options.a")
  set_target_properties(  boost_filesystem        PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_filesystem.a")
  set_target_properties(  boost_date_time         PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_date_time.a")
  set_target_properties(  boost_iostreams         PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_iostreams.a")
else()
  set_target_properties(  boost_program_options   PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_program_options.so")
  set_target_properties(  boost_filesystem        PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_filesystem.so")
  set_target_properties(  boost_date_time         PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_date_time.so")
  set_target_properties(  boost_iostreams         PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_iostreams.so")
endif()

link_libraries(
    boost_program_options
    boost_filesystem
    boost_date_time
    boost_iostreams
)

add_subdirectory(taq-prep)
add_subdirectory(taq-ctrl)
add_subdirectory(tick-calc)
add_subdirectory(taq-py)
