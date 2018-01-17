#!/bin/sh
pid=`pidof storage_center`
if [ ! -z $pid ]; then
		`kill $pid`
fi

path="/usr/local/sandai/storage_center"
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

echo "storage_center start."
nohup ./storage_center --conf="../conf/conf.json" >> $martini_log &
