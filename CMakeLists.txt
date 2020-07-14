cmake_minimum_required(VERSION 3.7)
project(taq-proc C CXX ASM-ATT)

#set(CMAKE_VERBOSE_MAKEFILE ON)

set(CXX_FLAGS "-std=c++17 -Wall -Werror -Wpedantic")
set(CXX_FLAGS_DEBUG   "-ggdb -Og")
set(CXX_FLAGS_RELEASE "-O3")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  ${CXX_FLAGS} ${CXX_FLAGS_DEBUG}")

add_definitions(-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)

set(BOOST_INCLUDE_PATH  /home/edaniley/Toolbox/boost_1_72_0 )
set(BOOST_LIB_PATH      /home/edaniley/Toolbox/boost_1_72_0/stage/lib )
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

link_libraries(
    boost_program_options
    boost_filesystem
    boost_date_time
)

add_subdirectory(taq-prep)
add_subdirectory(taq-ctrl)
add_subdirectory(tick-calc)
