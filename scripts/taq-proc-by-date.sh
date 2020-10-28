master=$(printf "/data/%s/raw/EQY_US_ALL_REF_MASTER_%s.gz\n" $1 $1)
/data/proc/taq-proc.sh $master

cmdfile=$(mktemp)
printf "/data/%s/raw/EQY_US_ALL_TRADE_%s.gz\n" $1 $1 > $cmdfile
for f in $(ls -S /data/$1/raw/SPLITS_US_ALL_BBO_*.gz); do
  printf "%s\n" $f >> $cmdfile
  printf "%s po\n" $f >> $cmdfile
done
cat $cmdfile |parallel -j12 --colsep ' ' /data/proc/taq-proc.sh {1} {2}
rm $cmdfile
