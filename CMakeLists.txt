cmake_minimum_required(VERSION 3.7)
project(taq-proc C CXX ASM-ATT)

#set(CMAKE_VERBOSE_MAKEFILE ON)

set(CXX_FLAGS "-std=c++17 -Wall -Werror -Wpedantic")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CXX_FLAGS_EXTRA "-O3")
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CXX_FLAGS_EXTRA "-O0 -ggdb")
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS} ${CXX_FLAGS_EXTRA}")

add_definitions(-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)

set(BOOST_INCLUDE_PATH  /usr/local/astral/boost_1_72_0 )
set(BOOST_LIB_PATH      /usr/local/astral/boost_1_72_0/stage/lib )
set(NUMPY_INCLUDE_PATH  /usr/local/lib64/python3.8/site-packages/numpy/core/include )
set(PYBIND_INCLUDE_PATH /user/local/astral/pybind11/include )
set(PYTHON_INCLUDE_PATH /us/include/python3.8 )

include_directories(
    include
    ${BOOST_INCLUDE_PATH}
)

add_library(            boost_program_options   STATIC IMPORTED)
set_target_properties(  boost_program_options   PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_program_options.a")
add_library(            boost_filesystem        STATIC IMPORTED)
set_target_properties(  boost_filesystem        PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_filesystem.a")
add_library(            boost_date_time         STATIC IMPORTED)
set_target_properties(  boost_date_time         PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_date_time.a")
add_library(            boost_iostreams         STATIC IMPORTED)
set_target_properties(  boost_iostreams         PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_iostreams.a")


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
