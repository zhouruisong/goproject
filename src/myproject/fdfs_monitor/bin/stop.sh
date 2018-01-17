#!/bin/sh
pid=`pidof fdfs_monitor`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "fdfs_monitor stop."

