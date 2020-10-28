tdate=$(echo $(basename $1)|awk '{n=split($0,x,"_"); print substr(x[n],1,8)}')
fname=$(printf "/data/%s/raw/%s" $tdate $(basename $1))
if [ "$(echo $1|grep EQY_US_ALL_REF_MASTER_)" != "" ];then
  zcat $fname | taq-prep -o /data -t master -d $tdate 1>/dev/null
elif [ "$(echo $1|grep EQY_US_ALL_TRADE_)" != "" ];then
  zcat $fname | taq-prep -o /data -t trade -d $tdate 1>/dev/null
elif [ "$(echo $1|grep SPLITS_US_ALL_BBO_)" != "" ];then
  symgr=$(echo $(basename $1)|awk '{if(index($0,"_BBO_")>0){split($0,x,"_");print x[5]}}')
  if [ "$2" == "" ];then
    zcat $fname | taq-prep -o /data -t quote -d $tdate -s $symgr 1>/dev/null
  else
    zcat $fname | taq-prep -o /data -t quote-po -d $tdate -s $symgr 1>/dev/null
  fi
fi
