bin=/usr/local/astral/bin
echo $PATH|sed 's/:/\n/g'|grep "$bin" 2>&1 > /dev/null
if [ "$?" != "0" ];then
    export PATH=$bin:$PATH
fi
echo $PYTHONPATH|sed 's/:/\n/g'|grep "$bin" 2>&1 > /dev/null
if [ "$?" != "0" ];then
    export PYTHONPATH=$bin:$PYTHONPATH
fi
