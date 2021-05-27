#!/bin/bash
# run: bash gen0525.sh
# Use rm randomQuery/q-* and rm randomGraph/g-* to delete all graphs generated

for n in 5 6 7 8 9 10
do
    n0=`expr $n - 1`
    m=`expr $n \* $n0 / 4`
    l=`expr $n / 2`

    ./generator g ${n} ${m} ${l} 99 > test0525/g/g-${n}-${m}-${l}.txt
    ./generator q ${n} ${m} ${l} 99 > test0525/q/q-${n}-${m}-${l}.txt
done