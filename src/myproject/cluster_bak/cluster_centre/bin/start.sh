#!/bin/sh
pid=`pidof cluster_centre`
if [ ! -z $pid ]; then
		`kill $pid`
fi

path="/usr/local/sandai/cluster_centre"
logs="$path/logs"
martini_log="$logs/access.log"
#echo "$logs"

if [ ! -d "$logs" ];then
		mkdir -p $logs
		if [ ! 0 -eq $? ];then
				echo "mkdir $logs failed."
				exit $?
		fi
fi

echo "cluster_centre start."
nohup ./cluster_centre --conf="../conf/conf.json" >> $martini_log &
