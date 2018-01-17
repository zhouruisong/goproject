#!/bin/sh

num=$1

ES_PORT=3000
for ((a=1;a<=num;a++)); 
do
	sleep 1
	filesize=$(($RANDOM%100))
	filename=`echo "zhouruisong$a.mp4"`
	current_time=`date +"%Y-%m-%d %H:%M:%S"`
	ff_uri="http://twin14602.sandai.net/tools/coral/${filename}"

	echo "filesize = ${filesize}"
	echo "filename = ${filename}"
	echo "current_time = ${current_time}"
	echo "ff_uri = ${ff_uri}"

	CURL_POST="\"id\":${filesize},\"taskid\":\"${filename}\",\"filename\":\"${filename}\",\"filetype\":\"mp4\",\"filesize\":${filesize},\"domain\":\"www.wasu.cn\",\"status\":200,\"action\":\"UP\",\"md5type\":1,\"dnamemd5\":\"${filename}\",\"sourceurl\":\"${ff_uri}\",\"transcodingurl\":\"${ff_uri}\",\"filemd5\":\"${filename}\",\"indexmd5\":\"\",\"headmd5\":\"\",\"expirytime\":\"${current_time}\",\"createtime\":\"${current_time}\",\"exectime\":\"${current_time}\",\"cburl\":\"${ff_uri}\",\"ffuri\":\"${ff_uri}\",\"taskbranchstatus\":\"\",\"localserverdir\":\"\",\"tsurl\":\"\",\"type\":0,\"transcodinginfo\":\"0\",\"isbackup\":0"

	#`curl -X POST http://192.168.110.34:${ES_PORT}/uploadput -s -d"{\"tablename\":\"t_live2odv2_kuwo\", \"data\": {${CURL_POST}}}"`
	`curl -X POST http://192.168.110.34:${ES_PORT}/uploadput -s -d"{\"tablename\":\"t_live_fcup_source_wasu\", \"data\": {${CURL_POST}}}"`
	#`curl -X POST http://192.168.110.34:${ES_PORT}/uploadput -s -d"{\"tablename\":\"t_livefcup\", \"data\": {${CURL_POST}}}"`
done
