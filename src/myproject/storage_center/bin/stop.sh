#!/bin/sh
pid=`pidof storage_center`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "storage_center stop"

