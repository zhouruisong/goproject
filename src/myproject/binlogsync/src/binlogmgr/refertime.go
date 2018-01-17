package binlogmgr

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func CheckStartEndTime(mgr *BinLogMgr) bool {
	current := time.Now().Unix()
	t := time.Unix(current, 0)

	start := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(),
		mgr.start_time.hour, mgr.start_time.minute, mgr.start_time.second)
	t_start, _ := time.Parse("2006-01-02 15:04:05", start)

	ret_start := t_start.Unix()

	end := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(),
		mgr.end_time.hour, mgr.end_time.minute, mgr.end_time.second)
	t_end, _ := time.Parse("2006-01-02 15:04:05", end)

	ret_end := t_end.Unix()

	current_time := time.Unix(current, 0).Format("2006-01-02 15:04:05")
	mgr.Logger.Infof("start:%+v, current_time:%+v, end:%+v", start, current_time, end)

	rt := ((current >= ret_start) && (current <= ret_end))
	return rt
}

func GetSyncTime(mgr *BinLogMgr, starttime string, endtime string) bool {
	rt := timeItem(mgr, starttime, &mgr.start_time)
	if rt == false {
		return false
	}

	ret := timeItem(mgr, endtime, &mgr.end_time)
	if ret == false {
		return false
	}

	g_sync_part_time = !((mgr.start_time.hour == 0 && mgr.start_time.minute == 0) &&
		(mgr.end_time.hour == 23 && mgr.end_time.minute == 59))

	return true
}

func timeItem(mgr *BinLogMgr, timeinfo string, tv *TimeInfo) bool {
	value := strings.Split(timeinfo, ":")
	var hour, minute, second int
	count := len(value)
	if count != 2 && count != 3 {
		mgr.Logger.Errorf("%+v is invalid", timeinfo)
		return false
	}

	if count == 3 {
		hour, _ = strconv.Atoi(value[0])
		minute, _ = strconv.Atoi(value[1])
		second, _ = strconv.Atoi(value[2])
	} else {
		hour, _ = strconv.Atoi(value[0])
		minute, _ = strconv.Atoi(value[1])
	}

	if (hour < 0 || hour > 23) || (minute < 0 || minute > 59) || (second < 0 || second > 59) {
		mgr.Logger.Errorf("%+v is invalid", timeinfo)
		return false
	}

	tv.hour = hour
	tv.minute = minute
	tv.second = second

	//	mgr.Logger.Infof("tv:%+v", tv)
	return true
}
