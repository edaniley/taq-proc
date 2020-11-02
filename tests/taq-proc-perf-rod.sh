echo "job:single runs:10 parallel:1 file:sample.csv markouts:n/a function:ROD"
for i in {1..10}; do
  python3 test-rod.py -t 35.239.195.67:3090 -i sample.csv -s 4173312
done

cmdfile=$(mktemp)
echo "job:parallel-4 runs:10 parallel:4 file:sample.csv markouts:n/a function:ROD"
for i in {1..10}; do
  echo "-t35.239.195.67:3090 -isample.csv -s4173312" >> $cmdfile
done
for i in {1..10}; do
  cat $cmdfile | parallel -j4 --colsep ' ' python3 test-rod.py {1} {2} {3}
done

cp /dev/null $(mktemp)
echo "job:job:parallel-4 runs:10 parallel:10 file:sample.csv markouts:n/a function:ROD"
for i in {1..10}; do
  echo "-t35.239.195.67:3090 -isample.csv -s4173312" >> $cmdfile
done
for i in {1..10}; do
  cat $cmdfile | parallel -j4 --colsep ' ' python3 test-rod.py {1} {2} {3}
done
rm $cmdfile
