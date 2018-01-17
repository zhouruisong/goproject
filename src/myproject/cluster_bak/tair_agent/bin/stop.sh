#!/bin/sh
pid=`pidof tair_agent`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "tair_agent stop."

