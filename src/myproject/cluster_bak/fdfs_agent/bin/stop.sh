#!/bin/sh
pid=`pidof fdfs_agent`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "fdfs_agent stop."

