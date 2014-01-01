#!/bin/sh

python ParallelClassifier.py 4 0 &
p1=$!
sleep 5
python ParallelClassifier.py 4 1 &
p2=$!
python ParallelClassifier.py 4 2 &
p3=$!
python ParallelClassifier.py 4 3 &
p4=$!
wait $p1
wait $p2
wait $p3
wait $p4

