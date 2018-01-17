#!/bin/sh
pid=`pidof fdfs_agent`
if [ ! -z $pid ]; then
		`kill $pid`
fi

path="/usr/local/sandai/fdfs_agent"
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

echo "fdfs_agent start."
nohup ./fdfs_agent --conf="../conf/conf.json" >> $martini_log &
