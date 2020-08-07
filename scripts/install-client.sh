scripts="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
bin=$(printf "%s/bin" $taq_proc)
cd $bin
python setup.py install

