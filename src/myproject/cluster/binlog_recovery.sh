#!/bin/bash

if [ $# != 1 ];then
        echo "please input recovery storage ip"
        exit 1
fi

#while true
#do
#    echo -n "please confirm your storage ip $1 (y/n)"
#    read val
#    case $val in
#		y|Y)
#			break
#			;;
#		n|N)
#			;;
#		*)
#        	echo "invalid input"
#            ;;
#        esac
#done


num=0
storage_ip=$1
g_binlog_mark_name=""
g_src=""
app_path="/usr/local/sandai/tfdfs"
config_path="${app_path}/conf/storage.conf"
group_name=`grep "^group_name=" ${config_path} | awk -F "=" '{printf $2}'`
#echo "group_name = ${group_name}"

#获取track_ip
declare -a g_tracker_ip
declare -a g_mark

#捕获信号的处理
function trapSignal()
{
	trap "" SIGTSTP
	trap "" SIGHUP
	trap 'echo "exit";unlockGroup; exit 1' SIGINT
#	trap "" SIGINT
	trap "" SIGQUIT
}

function getTrackerIp()
{
	j=0
	IPS=`grep "^tracker_server=" ${config_path} | awk -F '=' '{print $2}' | awk -F ':' '{print $1}'`
	#echo $IPS
	for ip in $IPS;
	do
		g_tracker_ip[$j]=$ip
		j=$(($j+1))
	done
}


#检测storage_ip的状态，如果是ACTIVE， 解锁group
function checkStatus()
{	
	${app_path}/bin/fdfs_monitor ${config_path} list > ${app_path}/bin/status.log
	status=`grep "ip_addr = ${storage_ip}" ${app_path}/bin/status.log | awk '{print $5}'`
	if [[ $status == "ACTIVE" ]];then
		echo "${storage_ip} status is $status"
		`rm -fr ${app_path}/bin/status.log`
		return 0
	elif [[ $status == "OFFLINE" ]];then
		echo "${storage_ip} status is $status"
		`rm -fr ${app_path}/bin/status.log`
		return 1
	elif	 [[ $status == "DELETE" ]];then
		echo "${storage_ip} status is $status"
		`rm -fr ${app_path}/bin/status.log`
		return 3
	else
		echo "${storage_ip} status is $status"
	fi
}

function stopStorage()
{
	base_path=`grep "^base_path=" ${config_path} | awk -F "=" '{print $2}'`
	#echo "base_path = ${base_path}"
	fdfs_storaged_pid="${base_path}/data/fdfs_storaged.pid"
	
	if [[ -s ${fdfs_storaged_pid} ]];then
		echo "fdfs_storaged.pid exist"
		${app_path}/bin/fdfs_storaged ${config_path} stop
	else
		echo "fdfs_storaged.pid not exist"
		`ps -ef|grep fdfs_storaged |grep -v grep| awk '{print $2}' | xargs kill -9`
	fi
		
	ret=$?
	if [[ $ret -eq 0 ]];then
		echo "stop $storage_ip in $group_name successful"
	else
		echo "stop $storage_ip in $group_name failed"
	fi
	
	while (true)
	do
		checkStatus
		rt=$?
		
		if [[ $rt -eq 1 ]];then
			break
		fi
	done
}

#echo "storage ${storage_ip} syncing data and binlog successful
#在启动前要锁定当前的group防止binlog无限增长
function deleteStorage()
{
	${app_path}/bin/fdfs_monitor ${config_path} delete $group_name $storage_ip
	
	while (true)
	do
		checkStatus
		rt=$?
		
		if [[ $rt -eq 3 ]];then
			break
		fi
	done
}

#在启动前要锁定当前的group防止binlog无限增长
function addStorage()
{
	${app_path}/bin/fdfs_storaged ${config_path}
	ret=$?
	if [[ $ret -eq 0 ]];then
		echo "add $storage_ip in $group_name successful"
	else
		echo "add $storage_ip in $group_name failed"
	fi
}

#在启动前要锁定当前的group防止binlog无限增长
function lockGroup()
{
	for ((start=0; start< $num; start++));
	do
	#	echo "start = ${start}"
		${app_path}/bin/fdfs_modify_parameters ${config_path} group lock ${g_tracker_ip[$start]} ${group_name}
		ret=$?
		if [[ $ret -eq 0 ]];then
			echo "$group_name in ${g_tracker_ip[$start]} lock successful"
		else
			echo "$group_name in ${g_tracker_ip[$start]} lock failed"
	    	return 1
		fi
	done
	return 0
}

#unlock ${group_name} after stroage status is ACTIVE
function unlockGroup()
{
	for ((start=0; start< $num; start++));
	do
	#	echo "start = ${start}"
		${app_path}/bin/fdfs_modify_parameters ${config_path} group unlock ${g_tracker_ip[$start]} ${group_name}
		ret=$?
		if [[ $ret -eq 0 ]];then
			echo "$group_name in ${g_tracker_ip[$start]} unlock successful"
		else
			echo "$group_name in ${g_tracker_ip[$start]} unlock failed"
	    	return 1
		fi
	done

	return 0
}

function doMark()
{
	#获取当前的mark，备份一份
	base_path=`grep "^base_path=" ${config_path} | awk -F "=" '{print $2}'`
	#echo "base_path = ${base_path}"
	binlog_path="${base_path}/data/sync"
	#echo "binlog_path = ${binlog_path}"
	mark_name="${storage_ip}_23000.mark"
	
	#获取当前时间
	houzhui=`date "+%Y%m%d_%H%M%S"`
	mark_name_bak="${mark_name}_${houzhui}"
	g_src="${binlog_path}/${mark_name}"
	des="${binlog_path}/${mark_name_bak}"
	`cp ${g_src} ${des}`
	
	g_binlog_mark_name=${g_src}
	
	#获取mark中的数值，为后面替换这些值做准备
	i=0
	VALUES=`cat ${g_src} | awk -F "=" '{print $2}'`
	#echo $VALUES
	
	for v in $VALUES;
	do
		g_mark[$i]=$v
		i=$(($i+1))
	done
}

function doBinlog()
{	
	#获取当前的binglog index，打开它获取到最后一个时间记录
	binlog_index_file="${binlog_path}/binlog.index"
	binlog_index=`cat ${binlog_index_file}`
	index=`printf "%03d" ${binlog_index}`
	last_binlog="${binlog_path}/binlog.${index}"
	until_time=`tail -1 ${last_binlog} | awk '{print $1}'`
#	echo "last_binlog = ${last_binlog} until_time = ${until_time}"
	
	if [[ $until_time == "" && $binlog_index == 0 ]];then
		echo "last_binlog = ${last_binlog} binlog_index = ${binlog_index} is null"
		unlockGroup
		exit 1
	fi
	
	if [[ $until_time == "" ]];then
		echo "binlog_index = ${binlog_index} is null, need choose other index"
		binlog_index=$(($binlog_index-1))
		index=`printf "%03d" ${binlog_index}`
		last_binlog="${binlog_path}/binlog.${index}"
		until_time=`tail -1 ${last_binlog} | awk '{print $1}'`
	fi
	
	echo "last_binlog = ${last_binlog} until_time = ${until_time}"
#	exit 1

	#替换mark文件中的一些值
	sed -i "1s/${g_mark[0]}/0/;2s/${g_mark[1]}/0/;3s/${g_mark[2]}/1/;4s/${g_mark[3]}/0/;5s/${g_mark[4]}/${until_time}/;6s/${g_mark[5]}/0/;7s/${g_mark[6]}/0/" ${g_src}
}

#程序开始
trapSignal
getTrackerIp
num=${#g_tracker_ip[@]}

#锁定组，禁止数据再次选择这个组
lockGroup
ret=$?
if [[ $ret -ne 0 ]];then
	rt=unlockGroup
	if [[ $ret -ne 0 ]];then
		exit 1
	fi
	exit 1
fi

#防止正在写的数据修改binlog，锁定后睡眠60分钟
sleep 60

doMark
doBinlog

#打印状态和同步情况
while (true)
do
	sync_done=`sed -n '4p' ${g_src}`
	if [[ $sync_done == "sync_old_done=1" ]];then
		break
	fi
	
	checkStatus	
	echo "$sync_done"
	sleep 1
done

#解锁group
unlockGroup
ret=$?
if [[ $ret -ne 0 ]];then
	exit 1
fi

echo "storage ${storage_ip} recovery data and binlog successful"

`cat ${g_src}`

exit 1
