#!/bin/sh
pid=`pidof cluster_centre`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "cluster_centre stop."

