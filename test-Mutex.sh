#!/bin/sh
for MAX in 1 2 3 ; do
echo "$MAX per second"
for A in $(seq 1 20) ; do
	sleep 1
	for B in $(seq 1 $MAX) ; do perl test-Mutex.pl & done
done
done

wait