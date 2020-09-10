#!/bin/bash

source /opt/rh/devtoolset-8/enable

set -x
toolbox=/usr/local/astral

function install_pybind11 {
    cd $toolbox
    git clone --depth 1 --branch v2.5.0 https://github.com/pybind/pybind11.git
    cd pybind11
    mkdir build
    cd build
    cmake -DBoost_INCLUDE_DIR=$toolbox/boost_1_72_0 ..
    make -j8
}

# required by pybind
sudo pip3 install pytest
install_pybind11
