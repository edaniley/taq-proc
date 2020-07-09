cmake_minimum_required(VERSION 2.8.12)
project(taqpy)

set(CMAKE_VERBOSE_MAKEFILE ON)
if(MSVC)
  SET(WINVER_10 "-D_WIN32_WINNT=0x0A00")
  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${WINVER_10}")
	set(JSON_INCLUDE_PATH   	C:/Toolbox/jsoncpp/include )
	set(JSON_LIB_PATH       	C:/Toolbox/jsoncpp/build/lib/Release )
	set(BOOST_INCLUDE_PATH  	C:/Toolbox/boost/boost_1_72_0 )
	set(BOOST_LIB_PATH      	C:/Toolbox/boost/boost_1_72_0/stage/lib )

	set(NUMPY_INCLUDE_PATH              C:/Python/Python37/lib/site-packages/numpy/core/include )
	#set(XTL_INCLUDE_PATH                C:/Toolbox/xtl/include )
	#set(XTENSOR_INCLUDE_PATH            C:/Toolbox/xtensor/include )
	#set(XTENSOR_PYTHON_INCLUDE_PATH     C:/Toolbox/xtensor-python/include )
else()
	set(JSON_INCLUDE_PATH   	/home/edaniley/Toolbox/jsoncpp/include )
	set(JSON_LIB_PATH       	/home/edaniley/Toolbox/jsoncpp/build/lib/Release )
	set(BOOST_INCLUDE_PATH  	/home/edaniley/Toolbox/boost_1_72_0 )
	set(BOOST_LIB_PATH      	/home/edaniley/Toolbox/boost_1_72_0/stage/lib )

	set(NUMPY_INCLUDE_PATH      C:/Python/Python37/lib/site-packages/numpy/core/include )
endif()


include_directories(
  ${NUMPY_INCLUDE_PATH}
  ${JSON_INCLUDE_PATH}
  ${BOOST_INCLUDE_PATH}
)

link_directories (
  ${JSON_LIB_PATH}
  ${BOOST_LIB_PATH}
)

if(LINUX)
  add_library(            jsoncpp   STATIC IMPORTED)
  set_target_properties(  jsoncpp   PROPERTIES IMPORTED_LOCATION "${JSON_LIB_PATH}/libjsoncpp.a")

  add_library(            boost_iostreams   STATIC IMPORTED)
  set_target_properties(  boost_iostreams   PROPERTIES IMPORTED_LOCATION "${BOOST_LIB_PATH}/libboost_boost_iostreams.a")
  

  link_libraries(
    jsoncpp
    boost_iostreams
  )
endif()

add_subdirectory(pybind11)
#add_subdirectory(C:/Toolbox/pybind11)

pybind11_add_module(
	taqpy
	src/main.cpp
)


