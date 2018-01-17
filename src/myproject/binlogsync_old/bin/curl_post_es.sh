#!/bin/sh

num=$1

ES_PORT=9200
for ((a=1;a<=num;a++)); 
do
	sleep 1
	filesize=$(($RANDOM%100))
	filename=`echo "zhouruisong$a.mp4"`
	current_time=`date +"%Y-%m-%dT%H:%M:%S+08:00"`
	ff_uri="http://twin14602.sandai.net/tools/coral/${filename}"

	echo "filesize = ${filesize}"
	echo "filename = ${filename}"
	echo "current_time = ${current_time}"
	echo "ff_uri = ${ff_uri}"

	CURL_POST="\"task_id\":\"${filename}\",\"action\":1,\"domain\":\"www.wasu.cn\",\"filename\":\"${filename}\",\"file_size\":${filesize},\"create_time\":\"${current_time}\",\"ff_uri\":\"${ff_uri}\""
	`curl -X POST http://192.168.110.30:${ES_PORT}/www.wasu.cn/DEL/ -s -d"{${CURL_POST}}"`
done
