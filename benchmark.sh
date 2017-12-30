#!/bin/sh
if [ "$1" = "start" ]
then
	if [ "$2" = "" ]
	then
		n=1
	else
		n=$2
	fi

	for i in `seq 1 $n`
	do
		nohup ./bloomFilterDemo-v3.php BF-TEST 17179869185 16 sample-$i.txt > data/benchmark-$i.log 2>&1 &
	done
fi
if [ "$1" = "stop" ]
then
	ps auxf|grep php |grep bloom |grep -v grep |awk '{print $2}' |xargs -i kill {}
fi
# vim: set ts=4 sw=4 cindent nu :

