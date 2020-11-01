if [ "$1" == "" ];then
  tdate="20200331"
else
  tdate=$1
fi
for f in $(ls -1 /data/${tdate}/*.nbbo-po.*.dat); do
  for s in $(taq-ctrl -f $f --no-header --sort|cut -d, -f1|egrep -v ".* .*" |head -20);do
    # expected output :  symbol:AAPL time:04:00:00.082050877 bid:256.1 offer:258.2
    taq-ctrl -f $f --no-header -s $s |awk -v sym=$s 'BEGIN {
      tot = 0;
      cnt = 0;
    } {
      split($2, x, ":");
      hh = x[2];
      mm = x[3];
      if ( ((hh==9 && mm>30) || hh>10) && hh < 16) {
        bid = substr($(NF-1),5,99);
        ask = substr($NF,7,99);
        mid = (bid + ask) / 2;
        if (mid < 1000000) {
          tot += mid;
          cnt ++;
        }
      }
    } END {
      printf("%s,%f\n", sym, tot/cnt);
    }'
  done
done
