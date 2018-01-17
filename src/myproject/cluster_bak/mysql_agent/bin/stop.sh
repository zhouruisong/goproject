#!/bin/sh
pid=`pidof mysql_agent`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "mysql_agent stop."

