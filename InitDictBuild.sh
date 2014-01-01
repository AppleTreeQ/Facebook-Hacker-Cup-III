#!/bin/sh

python ParallelFeatureDictionaryBuilder.py $1 $2 4 0 &
p1=$!
sleep 5
python ParallelFeatureDictionaryBuilder.py $1 $2 4 1 &
p2=$!
python ParallelFeatureDictionaryBuilder.py $1 $2 4 2 &
p3=$!
python ParallelFeatureDictionaryBuilder.py $1 $2 4 3 &
p4=$!
wait $p1
wait $p2
wait $p3
wait $p4

