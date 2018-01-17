#!/bin/sh
pid=`pidof cluster_backup`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "cluster_backup stop."

