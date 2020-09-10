scripts="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
taq_proc=$(dirname $scripts)
bin=$(printf "%s/bin" $taq_proc)

toolbox=/usr/local/astral

if [ ! -d $toolbox ]; then
   sudo mkdir $toolbox
   sudo chown $USER $toolbox
   sudo chgrp $USER $toolbox
   printf "Created %s\n" $toolbox
   ls -ld $toolbox
fi

if [ ! -d $toolbox/bin ]; then
   mkdir $toolbox/bin
fi

cp -p $taq_proc/bin/taq-prep $toolbox/bin/
cp -p $taq_proc/bin/taq-ctrl $toolbox/bin/
cp -p $taq_proc/bin/tick-calc $toolbox/bin/
cp -p $taq_proc/bin/taqpy.so $toolbox/bin/

source $taq_proc/scripts/env.sh
