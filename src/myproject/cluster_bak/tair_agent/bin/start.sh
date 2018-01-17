#!/bin/sh
pid=`pidof tair_agent`
if [ ! -z $pid ]; then
		`kill $pid`
fi

path="/usr/local/sandai/tair_agent"
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

echo "tair_agent start."
nohup ./tair_agent --conf="../conf/conf.json" >> $martini_log &
