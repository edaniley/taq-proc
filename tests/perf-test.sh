#!/bin/bash

# 2 20 4 perf-with-markouts-4.100k.hdf  m4 NBBOPrice,VWAP
# python3 taq-proc-perf-exec.py -d 20200331 -i perf-with-markouts-4.1m.hdf -f NBBOPrice,VWAP,NBBOPrice -m0

filename='perf-test-case.lst'
#n=1
while read line; do
  job_no=$(echo $line|awk '{print $1}')
  run_cnt=$(echo $line|awk '{print $2}')
  j=$(echo $line|awk '{print $3}')
  hdf="-i"$(echo $line|awk '{print $4}')
  markouts="-"$(echo $line|awk '{print $5}')
  function="-f"$(echo $line|awk '{print $6}')
  printf "job:%s runs:%s parallel:%s file:%s markouts:%s function:%s\n" $job_no $run_cnt $j $hdf $markouts $function
  #n=$((run_cnt * 1))
  cmdfile=$(mktemp)
  #run_cnt=1 # for quick test
  for (( i=1; i<=$run_cnt; i++ )); do
    for d in $(cat dates.2020.lst|grep 202003); do
      out="-o"$job_no"-"$d".hdf"
      d="-d"$d
      printf "%s %s %s %s %s\n" $d $hdf $markouts $function $out >> $cmdfile
    done
  done
  cat $cmdfile | parallel -j$j --colsep ' '  python3 taq-proc-perf-exec.py {1} {2} {3} {4} #{5}
  rm $cmdfile
done < $filename
