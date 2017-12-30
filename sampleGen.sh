#!/bin/sh
if [ "$1" = "" ]
then
	n=1000000
else
	n=$1
fi
if [ "$2" = "" ]
then
	k=1
else
	k=$2
fi

for i in `seq 1 $k`
do
	nohup ./sampleGen.php $n > data/sample-$i.txt 2>/dev/null &
done
# vim: set ts=4 sw=4 cindent nu :

