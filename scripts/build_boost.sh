#!/bin/bash

source /opt/rh/devtoolset-8/enable

set -x
toolbox=/usr/local/astral

if [ ! -d $toolbox ]; then
   sudo mkdir $toolbox
   sudo chown $USER $toolbox
   sudo chgrp $USER $toolbox
   printf "Created %s\n" $toolbox
   ls -ld $toolbox
   echo $USER
fi

function build_boost {
    cd $toolbox
    wget https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.gz &> /dev/null
    tar -zxvf boost_1_72_0.tar.gz &> /dev/null
    ls -lah
    rm boost_1_72_0.tar.gz
    cd boost_1_72_0
    ./bootstrap.sh           # to configure build system for current platform
    ./b2                     # to build C++ boost libraries
    rm -r bin.v2
}

build_boost
