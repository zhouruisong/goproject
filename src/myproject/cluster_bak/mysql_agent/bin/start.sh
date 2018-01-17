#!/bin/sh
pid=`pidof mysql_agent`
if [ ! -z $pid ]; then
		`kill $pid`
fi

path="/usr/local/sandai/mysql_agent"
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

echo "mysql_agent start."
nohup ./mysql_agent --conf="../conf/conf.json" >> $martini_log &
