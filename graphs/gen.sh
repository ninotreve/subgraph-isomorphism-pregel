#!/bin/bash
# Use rm randomQuery/q-* and rm randomGraph/g-* to delete all graphs generated

# n = 6
for item in 10
do
    ./generator g 6 ${item} 4 1 > randomGraph/g-6-${item}-4-1.txt
    ./generator q 6 ${item} 4 1 > randomQuery/q-6-${item}-4-1.txt
done

# n = 8
for item in 10 15 20
do
    ./generator g 8 ${item} 4 1 > randomGraph/g-8-${item}-4-1.txt
    ./generator q 8 ${item} 4 1 > randomQuery/q-8-${item}-4-1.txt
done

# n = 10
for item in 15 20 25 30
do
    ./generator g 10 ${item} 4 1 > randomGraph/g-10-${item}-4-1.txt
    ./generator q 10 ${item} 4 1 > randomQuery/q-10-${item}-4-1.txt
done
