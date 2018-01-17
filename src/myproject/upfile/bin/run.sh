#!/bin/sh
if [ $# != 1 ];then
	echo "please input appname"
	exit 1
fi
pid=`pidof $1`
if [ ! -z $pid ]; then
	`kill $pid`
fi

path="/usr/local/sandai/upfile"
logs="$path/logs"
record="$path/record"
martini_log="$logs/access.log"
#echo "$logs"

if [ ! -d "$logs" ];then
	mkdir -p $logs
	if [ ! 0 -eq $? ];then
		echo "mkdir $logs failed."
		exit $?
	fi
fi

record="$path/record"
if [ ! -d "$record" ];then
	mkdir -p $record
	if [ ! 0 -eq $? ];then
		echo "mkdir $record failed."
		exit $?
	fi
fi

echo "$1 start."
nohup ./$1 --conf="../conf/conf.json" >> $martini_log &
