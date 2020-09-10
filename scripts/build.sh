#!/bin/bash

source /opt/rh/devtoolset-8/enable

set -x

scripts="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
taq_proc=$(dirname $scripts)
build=$(printf "%s/build" $taq_proc)
bin=$(printf "%s/bin" $taq_proc)

if [ ! -d $build ]; then
   mkdir $build
fi     
if [ ! -d $bin ]; then
   mkdir $bin
fi 

cd $build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make  -j2

cp -p taq-prep/taq-prep ../bin
cp -p taq-ctrl/taq-ctrl ../bin
cp -p tick-calc/tick-calc ../bin
cp -p taq-py/libtaqpy.so ../bin/taqpy.so
echo $PATH|sed 's/:/\n/g'|grep $bin 2>&1 > /dev/null
if [ "$?" != "0" ]; then
   printf "\e[31m \e[5m NB! \e[25m \e[39m Please add %s to PATH \n" $bin
fi

