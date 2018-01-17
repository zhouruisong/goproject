#!/bin/sh
pid=`pidof cluster-test`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "cluster-test stop."

